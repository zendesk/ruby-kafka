module Kafka
  class Message
    attr_reader :value, :key, :topic, :partition

    def initialize(value, key:, topic:, partition:)
      @value = value
      @key = key
      @topic = topic
      @partition = partition
    end
  end
end
