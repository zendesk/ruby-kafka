module Kafka
  class PendingMessage
    attr_reader :value, :key, :topic, :partition, :partition_key

    attr_reader :bytesize, :create_time

    def initialize(value:, key:, topic:, partition:, partition_key:, create_time:)
      @key = key
      @value = value
      @topic = topic
      @partition = partition
      @partition_key = partition_key
      @create_time = create_time

      @bytesize = key.to_s.bytesize + value.to_s.bytesize
    end
  end
end
