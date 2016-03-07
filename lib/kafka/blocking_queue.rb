module Kafka
  class BlockingQueue
    class Overflow < StandardError
    end

    def initialize(max_length: nil)
      @mutex = Mutex.new
      @queue = []
      @item_added = ConditionVariable.new
      @item_removed = ConditionVariable.new
      @max_length = max_length
    end

    def enq(item, timeout: nil)
      @mutex.synchronize do
        if full?
          # If `timeout` is nil, we'll wait indefinitely. Otherwise we'll return
          # after the timeout expires.
          @item_removed.wait(@mutex, timeout) if timeout != 0

          raise Overflow, "Queue is full" if full?
        end

        @queue << item
        @item_added.signal
      end
    end

    alias << enq

    def deq(timeout: nil)
      @mutex.synchronize do
        if @queue.empty?
          # If `timeout` is nil, we'll wait indefinitely. Otherwise we'll return
          # after the timeout expires.
          @item_added.wait(@mutex, timeout) if timeout != 0

          raise ThreadError, "Queue empty" if @queue.empty?
        end

        item = @queue.shift

        @item_removed.signal

        item
      end
    end

    alias pop deq

    def length
      @queue.length
    end

    alias size length

    def full?
      @max_length && length >= @max_length
    end
  end
end
