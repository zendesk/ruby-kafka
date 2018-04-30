# frozen_string_literal: true

module Kafka

  class PendingMessageQueue
    attr_reader :size, :bytesize

    def initialize
      clear
    end

    def write(message)
      @messages << message
      @size += 1
      @bytesize += message.bytesize
    end

    def empty?
      @messages.empty?
    end

    def clear
      @messages = []
      @size = 0
      @bytesize = 0
    end

    def replace(messages)
      clear
      messages.each {|message| write(message) }
    end

    # Yields each message in the queue.
    #
    # @yieldparam [PendingMessage] message
    # @return [nil]
    def each(&block)
      @messages.each(&block)
    end
  end
end
