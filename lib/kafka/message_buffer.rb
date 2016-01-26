module Kafka

  # Buffers messages for specific topics/partitions.
  class MessageBuffer
    include Enumerable

    def initialize
      @buffer = {}
    end

    def write(message, topic:, partition:)
      buffer_for(topic, partition) << message
    end

    def concat(messages, topic:, partition:)
      buffer_for(topic, partition).concat(messages)
    end

    def to_h
      @buffer
    end

    def size
      @buffer.values.inject(0) {|sum, messages| messages.values.flatten.size + sum }
    end

    def empty?
      @buffer.empty?
    end

    def each
      @buffer.each do |topic, messages_for_topic|
        messages_for_topic.each do |partition, messages_for_partition|
          yield topic, partition, messages_for_partition
        end
      end
    end

    def clear
      @buffer = {}
    end

    private

    def buffer_for(topic, partition)
      @buffer[topic] ||= {}
      @buffer[topic][partition] ||= []
    end
  end
end
