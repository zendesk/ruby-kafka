module Kafka

  # An ordered sequence of messages fetched from a Kafka partition.
  class FetchedBatch
    # @return [String]
    attr_reader :topic

    # @return [Integer]
    attr_reader :partition

    # @return [Integer] the offset of the most recent message in the partition.
    attr_reader :highwater_mark_offset

    # @return [Array<Kafka::FetchedMessage>]
    attr_reader :messages

    def initialize(topic:, partition:, highwater_mark_offset:, messages:)
      @topic = topic
      @partition = partition
      @highwater_mark_offset = highwater_mark_offset
      @messages = messages
    end

    def empty?
      @messages.empty?
    end

    def last_offset
      return -2 if messages.empty?
      messages.last.offset
    end

    def offset_lag
      if empty?
        0
      else
        highwater_mark_offset - last_offset
      end
    end
  end
end
