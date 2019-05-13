require 'forwardable'

# Basic implementation of a tagged logger that matches the API of
# ActiveSupport::TaggedLogging.

module Kafka
  class TaggedFormatter

    def initialize(formatter)
      @formatter = formatter
    end

    def call(severity, timestamp, progname, msg)
      @formatter.call(severity, timestamp, progname, "#{tags_text}#{msg}")
    end

    def tagged(*tags)
      new_tags = push_tags(*tags)
      yield self
    ensure
      pop_tags(new_tags.size)
    end

    def push_tags(*tags)
      tags.flatten.reject { |t| t.nil? || t.empty? }.tap do |new_tags|
        current_tags.concat new_tags
      end
    end

    def pop_tags(size = 1)
      current_tags.pop size
    end

    def clear_tags!
      current_tags.clear
    end

    def current_tags
      # We use our object ID here to avoid conflicting with other instances
      thread_key = @thread_key ||= "kafka_tagged_logging_tags:#{object_id}".freeze
      Thread.current[thread_key] ||= []
    end

    def tags_text
      tags = current_tags
      if tags.any?
        tags.collect { |tag| "[#{tag}] " }.join
      end
    end

  end

  class TaggedLogger
    extend Forwardable
    delegate [:push_tags, :pop_tags, :clear_tags!] => :formatter
    delegate [:info, :error, :warn, :fatal, :debug, :level, :level=, :progname,
              :datetime_format=, :datetime_format, :sev_threshold,
              :sev_threshold=, :formatter, :debug?, :info?,
              :warn?, :error?, :fatal?, :reopen, :add, :log, :<<,
              :unknown, :close] => :@logger

    def formatter=(formatter)
      @logger.formatter = if formatter.is_a?(TaggedFormatter)
        formatter
      else
        TaggedFormatter.new(formatter || Logger::Formatter.new)
                       end
    end

    def self.new(logger_or_stream = nil)
      # don't keep wrapping the same logger over and over again
      return logger_or_stream if logger_or_stream.is_a?(TaggedLogger)
      super
    end

    def initialize(logger_or_stream = nil)
      @logger = if logger_or_stream.is_a?(Logger)
        logger_or_stream.clone
      elsif logger_or_stream
        Logger.new(logger_or_stream)
      else
        Logger.new(nil)
                end
      self.formatter = @logger.formatter
    end

    def tagged(*tags)
      formatter.tagged(*tags) { yield self }
    end

    def flush
      clear_tags!
      super if defined?(super)
    end
  end

end
