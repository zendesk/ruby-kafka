require "kafka/partitioner"
require "kafka/message_buffer"
require "kafka/protocol/message"

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
      @buffer = MessageBuffer.new
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
    # @return [nil]
    def write(value, key:, topic:, partition: nil, partition_key: nil)
      if partition.nil?
        # If no explicit partition key is specified we use the message key instead.
        partition_key ||= key
        partitioner = Partitioner.new(@broker_pool.partitions_for(topic))
        partition = partitioner.partition_for_key(partition_key)
      end

      message = Protocol::Message.new(key: key, value: value)

      @buffer.write(message, topic: topic, partition: partition)

      partition
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

      @buffer.each do |topic, partition, messages|
        broker = @broker_pool.get_leader(topic, partition)

        @logger.debug "Current leader for #{topic}/#{partition} is #{broker}"

        messages_for_broker[broker] ||= MessageBuffer.new
        messages_for_broker[broker].concat(messages, topic: topic, partition: partition)
      end

      messages_for_broker.each do |broker, message_set|
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

      @buffer.clear

      nil
    end

    def shutdown
      @broker_pool.shutdown
    end
  end
end
