require "kafka/consumer_group"
require "kafka/offset_manager"
require "kafka/fetch_operation"

module Kafka

  # @note This code is still alpha level. Don't use this for anything important.
  #   The API may also change without warning.
  #
  # A client that consumes messages from a Kafka cluster in coordination with
  # other clients.
  #
  # A Consumer subscribes to one or more Kafka topics; all consumers with the
  # same *group id* then agree on who should read from the individual topic
  # partitions. When group members join or leave, the group synchronizes,
  # making sure that all partitions are assigned to a single member, and that
  # all members have some partitions to read from.
  #
  # ## Example
  #
  # A simple producer that simply writes the messages it consumes to the
  # console.
  #
  #     require "kafka"
  #
  #     kafka = Kafka.new(seed_brokers: ["kafka1:9092", "kafka2:9092"])
  #
  #     # Create a new Consumer instance in the group `my-group`:
  #     consumer = kafka.consumer(group_id: "my-group")
  #
  #     # Subscribe to a Kafka topic:
  #     consumer.subscribe("messages")
  #
  #     # Loop forever, reading in messages from all topics that have been
  #     # subscribed to.
  #     consumer.each_message do |message|
  #       puts message.topic
  #       puts message.partition
  #       puts message.key
  #       puts message.value
  #       puts message.offset
  #     end
  #
  class Consumer

    def initialize(cluster:, logger:, group:, offset_manager:, session_timeout:, heartbeat:)
      @cluster = cluster
      @logger = logger
      @group = group
      @offset_manager = offset_manager
      @session_timeout = session_timeout
      @heartbeat = heartbeat

      # Whether or not the consumer is currently consuming messages.
      @running = false
    end

    # Subscribes the consumer to a topic.
    #
    # Typically you either want to start reading messages from the very
    # beginning of the topic's partitions or you simply want to wait for new
    # messages to be written. In the former case, set `default_offsets` to
    # `:earliest` (the default); in the latter, set it to `:latest`.
    #
    # @param topic [String] the name of the topic to subscribe to.
    # @param default_offset [Symbol] whether to start from the beginning or the
    #   end of the topic's partitions.
    # @return [nil]
    def subscribe(topic, default_offset: :earliest)
      @group.subscribe(topic)
      @offset_manager.set_default_offset(topic, default_offset)

      nil
    end

    def stop
      @running = false
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each message is yielded to the provided block. If the block returns
    # without raising an exception, the message will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from the server; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_wait_time [Integer] the maximum duration of time to wait before
    #   returning messages from the server, in seconds.
    # @yieldparam message [Kafka::FetchedMessage] a message fetched from Kafka.
    # @return [nil]
    def each_message(min_bytes: 1, max_wait_time: 5)
      consumer_loop do
        batches = fetch_batches(min_bytes: min_bytes, max_wait_time: max_wait_time)

        batches.each do |batch|
          batch.messages.each do |message|
            Instrumentation.instrument("process_message.consumer.kafka") do |notification|
              notification.update(
                topic: message.topic,
                partition: message.partition,
                offset: message.offset,
                offset_lag: batch.highwater_mark_offset - message.offset,
                key: message.key,
                value: message.value,
              )

              yield message
            end

            @offset_manager.commit_offsets_if_necessary

            @heartbeat.send_if_necessary
            mark_message_as_processed(message)

            return if !@running
          end
        end
      end
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each batch of messages is yielded to the provided block. If the block returns
    # without raising an exception, the batch will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message batch in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from the server; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_wait_time [Integer] the maximum duration of time to wait before
    #   returning messages from the server, in seconds.
    # @yieldparam batch [Kafka::FetchedBatch] a message batch fetched from Kafka.
    # @return [nil]
    def each_batch(min_bytes: 1, max_wait_time: 5)
      consumer_loop do
        batches = fetch_batches(min_bytes: min_bytes, max_wait_time: max_wait_time)

        batches.each do |batch|
          unless batch.empty?
            Instrumentation.instrument("process_batch.consumer.kafka") do |notification|
              notification.update(
                topic: batch.topic,
                partition: batch.partition,
                highwater_mark_offset: batch.highwater_mark_offset,
                message_count: batch.messages.count,
              )

              yield batch
            end

            mark_message_as_processed(batch.messages.last)
          end

          @offset_manager.commit_offsets_if_necessary

          @heartbeat.send_if_necessary

          return if !@running
        end
      end
    end

    private

    def consumer_loop
      @running = true

      while @running
        begin
          yield
        rescue HeartbeatError, OffsetCommitError, FetchError
          join_group
        end
      end
    ensure
      # In order to quickly have the consumer group re-balance itself, it's
      # important that members explicitly tell Kafka when they're leaving.
      @offset_manager.commit_offsets
      @group.leave
      @running = false
    end

    def join_group
      @offset_manager.clear_offsets
      @group.join
    end

    def fetch_batches(min_bytes:, max_wait_time:)
      join_group unless @group.member?

      assigned_partitions = @group.assigned_partitions

      @heartbeat.send_if_necessary

      raise "No partitions assigned!" if assigned_partitions.empty?

      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: min_bytes,
        max_wait_time: max_wait_time,
      )

      assigned_partitions.each do |topic, partitions|
        partitions.each do |partition|
          offset = @offset_manager.next_offset_for(topic, partition)

          @logger.debug "Fetching batch from #{topic}/#{partition} starting at offset #{offset}"

          operation.fetch_from_partition(topic, partition, offset: offset)
        end
      end

      operation.execute
    rescue ConnectionError => e
      @logger.error "Connection error while fetching messages: #{e}"

      raise FetchError, e
    end

    def mark_message_as_processed(message)
      @offset_manager.mark_as_processed(message.topic, message.partition, message.offset)
    end
  end
end
