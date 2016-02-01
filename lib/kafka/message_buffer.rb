module Kafka

  # Buffers messages for specific topics/partitions.
  class MessageBuffer
    include Enumerable

    attr_reader :size

    def initialize
      @buffer = {}
      @size = 0
    end

    def write(message, topic:, partition:)
      @size += 1
      buffer_for(topic, partition) << message
    end

    def concat(messages, topic:, partition:)
      @size += messages.count
      buffer_for(topic, partition).concat(messages)
    end

    def to_h
      @buffer
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

    # Clears buffered messages for the given topic and partition.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition id.
    #
    # @return [nil]
    def clear_messages(topic:, partition:)
      @size -= @buffer[topic][partition].count

      @buffer[topic].delete(partition)
      @buffer.delete(topic) if @buffer[topic].empty?
    end

    # Clears messages across all topics and partitions.
    #
    # @return [nil]
    def clear
      @buffer = {}
      @size = 0
    end

    private

    def buffer_for(topic, partition)
      @buffer[topic] ||= {}
      @buffer[topic][partition] ||= []
    end
  end
end
