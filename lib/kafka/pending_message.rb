# frozen_string_literal: true

module Kafka
  class PendingMessage
    attr_reader :value, :key, :headers, :topic, :partition, :partition_key,
                :create_time, :bytesize, :metadata

    def initialize(value:, key:, headers: {}, topic:, partition:, partition_key:,
                   create_time:, metadata: nil)
      @value = value
      @key = key
      @headers = headers
      @topic = topic
      @partition = partition
      @partition_key = partition_key
      @create_time = create_time
      @bytesize = key.to_s.bytesize + value.to_s.bytesize
      @metadata = metadata
    end

    def ==(other)
      @value == other.value &&
        @key == other.key &&
        @topic == other.topic &&
        @headers == other.headers &&
        @partition == other.partition &&
        @partition_key == other.partition_key &&
        @create_time == other.create_time &&
        @bytesize == other.bytesize
    end
  end
end
