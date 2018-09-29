module Kafka
  class LockedQueue
    def initialize
      @array = []
      @mutex = Mutex.new
      @resource = ConditionVariable.new
    end

    def <<(item)
      @mutex.synchronize do
        @array << item
        @resource.signal
      end
    end

    def empty?
      size == 0
    end

    def size
      @mutex.synchronize do
        @array.length
      end
    end

    def clear
      @mutex.synchronize do
        @array = []
      end
    end

    def map!(&block)
      @mutex.synchronize do
        @array.map!(&block)
      end
    end

    def deq
      @mutex.synchronize do
        if @array.empty?
          @resource.wait(@mutex)
        end
        @array.shift
      end
    end
  end
end
