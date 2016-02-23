module Kafka
  class PendingMessage
    attr_reader :value, :key, :topic, :partition, :partition_key

    attr_reader :bytesize

    def initialize(value:, key:, topic:, partition:, partition_key:)
      @key = key
      @value = value
      @topic = topic
      @partition = partition
      @partition_key = partition_key

      @bytesize = key.to_s.bytesize + value.to_s.bytesize
    end
  end
end
