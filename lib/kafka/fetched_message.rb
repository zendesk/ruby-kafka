# frozen_string_literal: true

module Kafka
  class FetchedMessage
    # @return [String] the name of the topic that the message was written to.
    attr_reader :topic

    # @return [Integer] the partition number that the message was written to.
    attr_reader :partition

    def initialize(message:, topic:, partition:)
      @message = message
      @topic = topic
      @partition = partition
    end

    # @return [String] the value of the message.
    def value
      @message.value
    end

    # @return [String] the key of the message.
    def key
      @message.key
    end

    # @return [Integer] the offset of the message in the partition.
    def offset
      @message.offset
    end

    # @return [Time] the timestamp of the message.
    def create_time
      @message.create_time
    end

    # @return [Hash<String, String>] the headers of the message.
    def headers
      @message.headers
    end

    # @return [Boolean] whether this record is a control record
    def is_control_record
      @message.is_control_record
    end

  end
end
