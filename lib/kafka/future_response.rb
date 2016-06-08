module Kafka
  class FutureResponse
    def initialize(&block)
      @block = block
      @value = nil
    end

    def value
      @value ||= @block.call
    end
  end
end
