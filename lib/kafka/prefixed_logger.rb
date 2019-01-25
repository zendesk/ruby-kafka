module Kafka
  class PrefixedLogger
    def initialize(prefix, logger)
      @prefix = prefix
      @logger = logger
    end

    [:unknown, :fatal, :error, :warn, :info, :debug].each do |level|
      class_eval <<-RUBY, __FILE__, __LINE__ + 1
         def #{level}(message)
           @logger.#{level}("[\#{@prefix}] \#{message}")
         end
      RUBY
    end
  end
end
