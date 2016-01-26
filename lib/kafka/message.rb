module Kafka
  class Message
    attr_reader :value, :key, :topic, :partition

    def initialize(value, topic, key:, partition:)
      @value     = value
      @topic     = topic
      @key       = key
      @partition = partition
    end
  end
end
