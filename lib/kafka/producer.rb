require "kafka/protocol/message"

module Kafka
  class Producer
    def initialize(cluster:, logger:)
      @cluster = cluster
      @logger = logger
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
