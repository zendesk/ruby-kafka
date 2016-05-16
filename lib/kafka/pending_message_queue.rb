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

    def prune_oldest
      prune_factor = 0.2 # By default, prune 20% of messages.
      target_size = @messages.size - (@messages.size * prune_factor).ceil

      until @messages.size <= target_size
        message = @messages.shift
        @size -= 1
        @bytesize -= message.bytesize
      end
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
