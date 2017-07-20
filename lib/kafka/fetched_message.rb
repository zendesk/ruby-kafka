module Kafka
  class FetchedMessage

    # @return [String] the value of the message.
    attr_reader :value

    # @return [String] the key of the message.
    attr_reader :key

    # @return [String] the name of the topic that the message was written to.
    attr_reader :topic

    # @return [Integer] the partition number that the message was written to.
    attr_reader :partition

    # @return [Integer] the offset of the message in the partition.
    attr_reader :offset

    # @return [Time] the timestamp of the message.
    attr_reader :create_time

    def initialize(value:, key:, topic:, partition:, offset:, create_time:)
      @value = value
      @key = key
      @topic = topic
      @partition = partition
      @offset = offset
      @create_time = create_time
    end
  end
end
