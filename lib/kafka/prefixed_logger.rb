module Kafka
  class PrefixedLogger < SimpleDelegator
    def initialize(prefix, logger)
      @prefix = prefix

      super(logger)
    end

    %w[unknown fatal error warn info debug].each do |method|
      define_method(method) do |message|
        message = "[#{@prefix}] #{message}"
        __getobj__.public_send(method, message)
      end
    end
  end
end
