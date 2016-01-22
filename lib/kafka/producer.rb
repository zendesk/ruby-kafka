require "kafka/message"
require "kafka/message_set"
require "kafka/partitioner"

module Kafka
  class Producer
    # @param timeout [Integer] The number of seconds to wait for an
    #   acknowledgement from the broker before timing out.
    # @param required_acks [Integer] The number of replicas that must acknowledge
    #   a write.
    def initialize(broker_pool:, logger:, timeout: 10, required_acks: 1)
      @broker_pool = broker_pool
      @logger = logger
      @required_acks = required_acks
      @timeout = timeout
      @buffered_messages = []
    end

    # Writes a message to the specified topic. Note that messages are buffered in
    # the producer until {#flush} is called.
    #
    # == Partitioning
    #
    # There are several options for specifying the partition that the message should
    # be written to. The simplest option is to not specify a partition or partition
    # key, in which case the message key will be used to select one of the available
    # partitions. You can also specify the `partition` parameter yourself. This
    # requires you to know which partitions are available, however. Oftentimes the
    # best option is to specify the `partition_key` parameter: messages with the
    # same partition key will always be assigned to the same partition, as long as
    # the number of partitions doesn't change.
    #
    # @param value [String] the message data.
    # @param key [String] the message key.
    # @param topic [String] the topic that the message should be written to.
    # @param partition [Integer] the partition that the message should be written to.
    # @param partition_key [String] the key that should be used to assign a partition.
    #
    # @return [Message] the message that was written.
    def write(value, key:, topic:, partition: nil, partition_key: nil)
      if partition.nil?
        # If no explicit partition key is specified we use the message key instead.
        partition_key ||= key
        partitioner = Partitioner.new(@broker_pool.partitions_for(topic))
        partition = partitioner.partition_for_key(partition_key)
      end

      message = Message.new(value, key: key, topic: topic, partition: partition)

      @buffered_messages << message

      message
    end

    # Flushes all messages to the Kafka brokers.
    #
    # Depending on the value of `required_acks` used when initializing the producer,
    # this call may block until the specified number of replicas have acknowledged
    # the writes. The `timeout` setting places an upper bound on the amount of time
    # the call will block before failing.
    #
    # @return [nil]
    def flush
      messages_for_broker = {}

      @buffered_messages.each do |message|
        broker = @broker_pool.get_leader(message.topic, message.partition)

        messages_for_broker[broker] ||= []
        messages_for_broker[broker] << message
      end

      messages_for_broker.each do |broker, messages|
        @logger.info "Sending #{messages.count} messages to broker #{broker}"

        message_set = MessageSet.new(messages)

        response = broker.produce(
          messages_for_topics: message_set.to_h,
          required_acks: @required_acks,
          timeout: @timeout * 1000, # Kafka expects the timeout in milliseconds.
        )

        if response
          response.topics.each do |topic_info|
            topic_info.partitions.each do |partition_info|
              Protocol.handle_error(partition_info.error_code)
            end
          end
        end
      end

      @buffered_messages.clear

      nil
    end

    def shutdown
      @broker_pool.shutdown
    end
  end
end
