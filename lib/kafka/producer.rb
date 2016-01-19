require "kafka/protocol/message"

module Kafka
  class Producer
    # @param timeout [Integer] The number of milliseconds to wait for an
    #   acknowledgement from the broker before timing out.
    # @param required_acks [Integer] The number of replicas that must acknowledge
    #   a write.
    def initialize(cluster:, logger:, timeout: 10_000, required_acks: 1)
      @cluster = cluster
      @logger = logger
      @required_acks = required_acks
      @timeout = timeout
      @buffer = {}
    end

    def write(value, key:, topic:, partition:)
      message = Protocol::Message.new(value: value, key: key)

      @buffer[topic] ||= {}
      @buffer[topic][partition] ||= []
      @buffer[topic][partition] << message
    end

    def flush
      @cluster.produce(
        required_acks: @required_acks,
        timeout: @timeout,
        messages_for_topics: @buffer
      )

      @buffer = {}
    end
  end
end
