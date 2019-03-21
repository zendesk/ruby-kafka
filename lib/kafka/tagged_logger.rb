require 'forwardable'

# Basic implementation of a tagged logger that matches the API of
# ActiveSupport::TaggedLogging.

module Kafka
  class TaggedLogger
    def self.new(logger)
      return logger if logger.is_a?(Kafka::TaggedLogger)
      return super(logger)
    end

    def initialize(logger)
      @logger = logger || Logger.new(nil)
    end

    def add(severity, message = nil, progname = nil)
      return true if (severity || ::Logger::UNKNOWN) < @logger.level

      if message.nil?
        if block_given?
          message = yield
        else
          message = progname
          progname = nil
        end
      end

      @logger.add(severity, "#{tags_text}#{message}", progname)
    end

    def clear_tags!
      current_tags.clear
    end

    def debug(progname = nil, &block)
      add(::Logger::DEBUG, nil, progname, &block)
    end

    def info(progname = nil, &block)
      add(::Logger::INFO, nil, progname, &block)
    end

    def error(progname = nil, &block)
      add(::Logger::ERROR, nil, progname, &block)
    end

    def fatal(progname = nil, &block)
      add(::Logger::FATAL, nil, progname, &block)
    end

    def flush
      clear_tags!
      @logger.flush if @logger.respond_to?(:flush)
    end

    def pop_tags(size = 1)
      current_tags.pop size
    end

    def push_tags(*tags)
      tags.flatten.reject { |t| t.nil? || t.empty? }.tap do |new_tags|
        current_tags.concat new_tags
      end
    end

    def tagged(*tags)
      new_tags = push_tags(*tags)
      yield self
    ensure
      pop_tags(new_tags.size)
    end

    def unknown(progname = nil, &block)
      add(::Logger::UNKNOWN, nil, progname, &block)
    end

    def warn(progname = nil, &block)
      add(::Logger::WARN, nil, progname, &block)
    end

    private

    def current_tags
      # We use our object ID here to avoid conflicting with other instances
      thread_key = @thread_key ||= "kafka_tagged_logging_tags:#{object_id}".freeze
      Thread.current[thread_key] ||= []
    end

    def format_message(severity, datetime, progname, msg)
      (@logger.formatter || @default_formatter).call(severity, datetime, progname, "#{tags_text}#{msg}")
    end

    def tags_text
      tags = current_tags
      if tags.any?
        tags.collect { |tag| "[#{tag}] " }.join
      end
    end
  end
end
