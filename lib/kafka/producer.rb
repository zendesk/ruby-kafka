# frozen_string_literal: true

require "set"
require "kafka/partitioner"
require "kafka/message_buffer"
require "kafka/produce_operation"
require "kafka/pending_message_queue"
require "kafka/pending_message"
require "kafka/compressor"
require "kafka/interceptors"

module Kafka
  # Allows sending messages to a Kafka cluster.
  #
  # Typically you won't instantiate this class yourself, but rather have {Kafka::Client}
  # do it for you, e.g.
  #
  #     # Will instantiate Kafka::Client
  #     kafka = Kafka.new(["kafka1:9092", "kafka2:9092"])
  #
  #     # Will instantiate Kafka::Producer
  #     producer = kafka.producer
  #
  # This is done in order to share a logger as well as a pool of broker connections across
  # different producers. This also means that you don't need to pass the `cluster` and
  # `logger` options to `#producer`. See {#initialize} for the list of other options
  # you can pass in.
  #
  # ## Buffering
  #
  # The producer buffers pending messages until {#deliver_messages} is called. Note that there is
  # a maximum buffer size (default is 1,000 messages) and writing messages after the
  # buffer has reached this size will result in a BufferOverflow exception. Make sure
  # to periodically call {#deliver_messages} or set `max_buffer_size` to an appropriate value.
  #
  # Buffering messages and sending them in batches greatly improves performance, so
  # try to avoid sending messages after every write. The tradeoff between throughput and
  # message delays depends on your use case.
  #
  # ## Error Handling and Retries
  #
  # The design of the error handling is based on having a {MessageBuffer} hold messages
  # for all topics/partitions. Whenever we want to send messages to the cluster, we
  # group the buffered messages by the broker they need to be sent to and fire off a
  # request to each broker. A request can be a partial success, so we go through the
  # response and inspect the error code for each partition that we wrote to. If the
  # write to a given partition was successful, we clear the corresponding messages
  # from the buffer -- otherwise, we log the error and keep the messages in the buffer.
  #
  # After this, we check if the buffer is empty. If it is, we're all done. If it's
  # not, we do another round of requests, this time with just the remaining messages.
  # We do this for as long as `max_retries` permits.
  #
  # ## Compression
  #
  # Depending on what kind of data you produce, enabling compression may yield improved
  # bandwidth and space usage. Compression in Kafka is done on entire messages sets
  # rather than on individual messages. This improves the compression rate and generally
  # means that compressions works better the larger your buffers get, since the message
  # sets will be larger by the time they're compressed.
  #
  # Since many workloads have variations in throughput and distribution across partitions,
  # it's possible to configure a threshold for when to enable compression by setting
  # `compression_threshold`. Only if the defined number of messages are buffered for a
  # partition will the messages be compressed.
  #
  # Compression is enabled by passing the `compression_codec` parameter with the
  # name of one of the algorithms allowed by Kafka:
  #
  # * `:snappy` for [Snappy](http://google.github.io/snappy/) compression.
  # * `:gzip` for [gzip](https://en.wikipedia.org/wiki/Gzip) compression.
  # * `:lz4` for [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression.
  # * `:zstd` for [zstd](https://facebook.github.io/zstd/) compression.
  #
  # By default, all message sets will be compressed if you specify a compression
  # codec. To increase the compression threshold, set `compression_threshold` to
  # an integer value higher than one.
  #
  # ## Instrumentation
  #
  # Whenever {#produce} is called, the notification `produce_message.producer.kafka`
  # will be emitted with the following payload:
  #
  # * `value` – the message value.
  # * `key` – the message key.
  # * `topic` – the topic that was produced to.
  # * `buffer_size` – the buffer size after adding the message.
  # * `max_buffer_size` – the maximum allowed buffer size for the producer.
  #
  # After {#deliver_messages} completes, the notification
  # `deliver_messages.producer.kafka` will be emitted with the following payload:
  #
  # * `message_count` – the total number of messages that the producer tried to
  #   deliver. Note that not all messages may get delivered.
  # * `delivered_message_count` – the number of messages that were successfully
  #   delivered.
  # * `attempts` – the number of attempts made to deliver the messages.
  #
  # ## Example
  #
  # This is an example of an application which reads lines from stdin and writes them
  # to Kafka:
  #
  #     require "kafka"
  #
  #     logger = Logger.new($stderr)
  #     brokers = ENV.fetch("KAFKA_BROKERS").split(",")
  #
  #     # Make sure to create this topic in your Kafka cluster or configure the
  #     # cluster to auto-create topics.
  #     topic = "random-messages"
  #
  #     kafka = Kafka.new(brokers, client_id: "simple-producer", logger: logger)
  #     producer = kafka.producer
  #
  #     begin
  #       $stdin.each_with_index do |line, index|
  #         producer.produce(line, topic: topic)
  #
  #         # Send messages for every 10 lines.
  #         producer.deliver_messages if index % 10 == 0
  #       end
  #     ensure
  #       # Make sure to send any remaining messages.
  #       producer.deliver_messages
  #
  #       producer.shutdown
  #     end
  #
  class Producer
    class AbortTransaction < StandardError; end

    def initialize(cluster:, transaction_manager:, logger:, instrumenter:, compressor:, ack_timeout:,
                   required_acks:, max_retries:, retry_backoff:, max_buffer_size:,
                   max_buffer_bytesize:, partitioner:, interceptors: [])
      @cluster = cluster
      @transaction_manager = transaction_manager
      @logger = TaggedLogger.new(logger)
      @instrumenter = instrumenter
      @required_acks = required_acks == :all ? -1 : required_acks
      @ack_timeout = ack_timeout
      @max_retries = max_retries
      @retry_backoff = retry_backoff
      @max_buffer_size = max_buffer_size
      @max_buffer_bytesize = max_buffer_bytesize
      @compressor = compressor
      @partitioner = partitioner
      @interceptors = Interceptors.new(interceptors: interceptors, logger: logger)

      # The set of topics that are produced to.
      @target_topics = Set.new

      # A buffer organized by topic/partition.
      @buffer = MessageBuffer.new

      # Messages added by `#produce` but not yet assigned a partition.
      @pending_message_queue = PendingMessageQueue.new
    end

    def to_s
      "Producer #{@target_topics.to_a.join(', ')}"
    end

    # Produces a message to the specified topic. Note that messages are buffered in
    # the producer until {#deliver_messages} is called.
    #
    # ## Partitioning
    #
    # There are several options for specifying the partition that the message should
    # be written to.
    #
    # The simplest option is to not specify a message key, partition key, or
    # partition number, in which case the message will be assigned a partition at
    # random.
    #
    # You can also specify the `partition` parameter yourself. This requires you to
    # know which partitions are available, however. Oftentimes the best option is
    # to specify the `partition_key` parameter: messages with the same partition
    # key will always be assigned to the same partition, as long as the number of
    # partitions doesn't change. You can also omit the partition key and specify
    # a message key instead. The message key is part of the message payload, and
    # so can carry semantic value--whether you want to have the message key double
    # as a partition key is up to you.
    #
    # @param value [String] the message data.
    # @param key [String] the message key.
    # @param headers [Hash<String, String>] the headers for the message.
    # @param topic [String] the topic that the message should be written to.
    # @param partition [Integer] the partition that the message should be written to.
    # @param partition_key [String] the key that should be used to assign a partition.
    # @param create_time [Time] the timestamp that should be set on the message.
    #
    # @raise [BufferOverflow] if the maximum buffer size has been reached.
    # @return [nil]
    def produce(value, key: nil, headers: {}, topic:, partition: nil, partition_key: nil, create_time: Time.now)
      # We want to fail fast if `topic` isn't a String
      topic = topic.to_str

      message = @interceptors.call(PendingMessage.new(
        value: value && value.to_s,
        key: key && key.to_s,
        headers: headers,
        topic: topic,
        partition: partition && Integer(partition),
        partition_key: partition_key && partition_key.to_s,
        create_time: create_time
      ))

      if buffer_size >= @max_buffer_size
        buffer_overflow topic,
          "Cannot produce to #{topic}, max buffer size (#{@max_buffer_size} messages) reached"
      end

      if buffer_bytesize + message.bytesize >= @max_buffer_bytesize
        buffer_overflow topic,
          "Cannot produce to #{topic}, max buffer bytesize (#{@max_buffer_bytesize} bytes) reached"
      end

      # If the producer is in transactional mode, all the message production
      # must be used when the producer is currently in transaction
      if @transaction_manager.transactional? && !@transaction_manager.in_transaction?
        raise "Cannot produce to #{topic}: You must trigger begin_transaction before producing messages"
      end

      @target_topics.add(topic)
      @pending_message_queue.write(message)

      @instrumenter.instrument("produce_message.producer", {
        value: value,
        key: key,
        topic: topic,
        create_time: create_time,
        message_size: message.bytesize,
        buffer_size: buffer_size,
        max_buffer_size: @max_buffer_size,
      })

      nil
    end

    # Sends all buffered messages to the Kafka brokers.
    #
    # Depending on the value of `required_acks` used when initializing the producer,
    # this call may block until the specified number of replicas have acknowledged
    # the writes. The `ack_timeout` setting places an upper bound on the amount of
    # time the call will block before failing.
    #
    # @raise [DeliveryFailed] if not all messages could be successfully sent.
    # @return [nil]
    def deliver_messages
      # There's no need to do anything if the buffer is empty.
      return if buffer_size == 0

      @instrumenter.instrument("deliver_messages.producer") do |notification|
        message_count = buffer_size

        notification[:message_count] = message_count
        notification[:attempts] = 0

        begin
          deliver_messages_with_retries(notification)
        ensure
          notification[:delivered_message_count] = message_count - buffer_size
        end
      end
    end

    # Returns the number of messages currently held in the buffer.
    #
    # @return [Integer] buffer size.
    def buffer_size
      @pending_message_queue.size + @buffer.size
    end

    def buffer_bytesize
      @pending_message_queue.bytesize + @buffer.bytesize
    end

    # Deletes all buffered messages.
    #
    # @return [nil]
    def clear_buffer
      @buffer.clear
      @pending_message_queue.clear
    end

    # Closes all connections to the brokers.
    #
    # @return [nil]
    def shutdown
      @transaction_manager.close
      @cluster.disconnect
    end

    # Initializes the producer to ready for future transactions. This method
    # should be triggered once, before any tranactions are created.
    #
    # @return [nil]
    def init_transactions
      @transaction_manager.init_transactions
    end

    # Mark the beginning of a transaction. This method transitions the state
    # of the transaction trantiions to IN_TRANSACTION.
    #
    # All producing operations can only be executed while the transation is
    # in this state. The records are persisted by Kafka brokers, but not visible
    # the consumers until the #commit_transaction method is trigger. After a
    # timeout period without committed, the transaction is timeout and
    # considered as aborted.
    #
    # @return [nil]
    def begin_transaction
      @transaction_manager.begin_transaction
    end

    # This method commits the pending transaction, marks all the produced
    # records committed. After that, they are visible to the consumers.
    #
    # This method can only be called if and only if the current transaction
    # is at IN_TRANSACTION state.
    #
    # @return [nil]
    def commit_transaction
      @transaction_manager.commit_transaction
    end

    # This method abort the pending transaction, marks all the produced
    # records aborted. All the records will be wiped out by the brokers and the
    # cosumers don't have a chance to consume those messages, except they enable
    # consuming uncommitted option.
    #
    # This method can only be called if and only if the current transaction
    # is at IN_TRANSACTION state.
    #
    # @return [nil]
    def abort_transaction
      @transaction_manager.abort_transaction
    end

    # Sends batch last offset to the consumer group coordinator, and also marks
    # this offset as part of the current transaction. This offset will be considered
    # committed only if the transaction is committed successfully.
    #
    # This method should be used when you need to batch consumed and produced messages
    # together, typically in a consume-transform-produce pattern. Thus, the specified
    # group_id should be the same as config parameter group_id of the used
    # consumer.
    #
    # @return [nil]
    def send_offsets_to_transaction(batch:, group_id:)
      @transaction_manager.send_offsets_to_txn(offsets: { batch.topic => { batch.partition => { offset: batch.last_offset + 1, leader_epoch: batch.leader_epoch } } }, group_id: group_id)
    end

    # Syntactic sugar to enable easier transaction usage. Do the following steps
    #
    # - Start the transaction (with Producer#begin_transaction)
    # - Yield the given block
    # - Commit the transaction (with Producer#commit_transaction)
    #
    # If the block raises exception, the transaction is automatically aborted
    # *before* bubble up the exception.
    #
    # If the block raises Kafka::Producer::AbortTransaction indicator exception,
    # it aborts the transaction silently, without throwing up that exception.
    #
    # @return [nil]
    def transaction
      raise 'This method requires a block' unless block_given?
      begin_transaction
      yield
      commit_transaction
    rescue Kafka::Producer::AbortTransaction
      abort_transaction
    rescue
      abort_transaction
      raise
    end

    private

    def deliver_messages_with_retries(notification)
      attempt = 0

      @cluster.add_target_topics(@target_topics)

      operation = ProduceOperation.new(
        cluster: @cluster,
        transaction_manager: @transaction_manager,
        buffer: @buffer,
        required_acks: @required_acks,
        ack_timeout: @ack_timeout,
        compressor: @compressor,
        logger: @logger,
        instrumenter: @instrumenter,
      )

      loop do
        attempt += 1

        notification[:attempts] = attempt

        begin
          @cluster.refresh_metadata_if_necessary!
        rescue ConnectionError => e
          raise DeliveryFailed.new(e, buffer_messages)
        end

        assign_partitions!
        operation.execute

        if @required_acks.zero?
          # No response is returned by the brokers, so we can't know which messages
          # have been successfully written. Our only option is to assume that they all
          # have.
          @buffer.clear
        end

        if buffer_size.zero?
          break
        elsif attempt <= @max_retries
          @logger.warn "Failed to send all messages to #{pretty_partitions}; attempting retry #{attempt} of #{@max_retries} after #{@retry_backoff}s"

          sleep @retry_backoff
        else
          @logger.error "Failed to send all messages to #{pretty_partitions}; keeping remaining messages in buffer"
          break
        end
      end

      unless @pending_message_queue.empty?
        # Mark the cluster as stale in order to force a cluster metadata refresh.
        @cluster.mark_as_stale!
        raise DeliveryFailed.new("Failed to assign partitions to #{@pending_message_queue.size} messages", buffer_messages)
      end

      unless @buffer.empty?
        raise DeliveryFailed.new("Failed to send messages to #{pretty_partitions}", buffer_messages)
      end
    end

    def pretty_partitions
      @buffer.map {|topic, partition, _| "#{topic}/#{partition}" }.join(", ")
    end

    def assign_partitions!
      failed_messages = []
      topics_with_failures = Set.new

      @pending_message_queue.each do |message|
        partition = message.partition

        begin
          # If a message for a topic fails to receive a partition all subsequent
          # messages for the topic should be retried to preserve ordering
          if topics_with_failures.include?(message.topic)
            failed_messages << message
            next
          end

          if partition.nil?
            partition_count = @cluster.partitions_for(message.topic).count
            partition = @partitioner.call(partition_count, message)
          end

          @buffer.write(
            value: message.value,
            key: message.key,
            headers: message.headers,
            topic: message.topic,
            partition: partition,
            create_time: message.create_time,
          )
        rescue Kafka::Error => e
          @instrumenter.instrument("topic_error.producer", {
            topic: message.topic,
            exception: [e.class.to_s, e.message],
          })

          topics_with_failures << message.topic
          failed_messages << message
        end
      end

      if failed_messages.any?
        failed_messages.group_by(&:topic).each do |topic, messages|
          @logger.error "Failed to assign partitions to #{messages.count} messages in #{topic}"
        end

        @cluster.mark_as_stale!
      end

      @pending_message_queue.replace(failed_messages)
    end

    def buffer_messages
      messages = []

      @pending_message_queue.each do |message|
        messages << message
      end

      @buffer.each do |topic, partition, messages_for_partition|
        messages_for_partition.each do |message|
          messages << PendingMessage.new(
            value: message.value,
            key: message.key,
            headers: message.headers,
            topic: topic,
            partition: partition,
            partition_key: nil,
            create_time: message.create_time
          )
        end
      end

      messages
    end

    def buffer_overflow(topic, message)
      @instrumenter.instrument("buffer_overflow.producer", {
        topic: topic,
      })

      raise BufferOverflow, message
    end
  end
end
