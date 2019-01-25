module Kafka
  class PrefixedLogger
    def initialize(prefix, logger)
      @prefix = prefix
      @logger = logger
      @tags = []
    end

    def with_tags(*tags)
      @tags = tags
      yield
    ensure
      @tags.clear
    end

    [:unknown, :fatal, :error, :warn, :info, :debug].each do |level|
      class_eval <<-RUBY, __FILE__, __LINE__ + 1
         def #{level}(message)
           @logger.#{level}(format(message))
         end
      RUBY
    end

    private

    def format(message)
      out = "[#{@prefix}] "
      @tags.each {|tag| out << "[#{tag}] " }

      out << message

      out
    end
  end
end
