module Kafka

  # A pending message queue holds messages that have not yet been assigned to
  # a partition. It's designed to only remove messages once they've been
  # successfully handled.
  class PendingMessageQueue
    attr_reader :size, :bytesize

    def initialize
      @messages = []
      @size = 0
      @bytesize = 0
    end

    def write(message)
      @messages << message
      @size += 1
      @bytesize += message.bytesize
    end

    def empty?
      @messages.empty?
    end

    # Yields each message in the queue to the provided block, removing the
    # message after the block has processed it. If the block raises an
    # exception, the message will be retained in the queue.
    #
    # @yieldparam [PendingMessage] message
    # @return [nil]
    def dequeue_each(&block)
      until @messages.empty?
        message = @messages.first

        yield message

        @size -= 1
        @bytesize -= message.bytesize
        @messages.shift
      end
    end
  end
end
