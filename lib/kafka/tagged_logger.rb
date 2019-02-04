require 'forwardable'

# Basic implementation of a tagged logger that matches the API of
# ActiveSupport::TaggedLogging.

module Kafka
  module TaggedFormatter

    def call(severity, timestamp, progname, msg)
      super(severity, timestamp, progname, "#{tags_text}#{msg}")
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

  module TaggedLogger
    extend Forwardable
    delegate [:push_tags, :pop_tags, :clear_tags!] => :formatter

    def self.new(logger)
      logger ||= Logger.new(nil)
      return logger if logger.respond_to?(:push_tags) # already included
      # Ensure we set a default formatter so we aren't extending nil!
      logger.formatter ||= Logger::Formatter.new
      logger.formatter.extend TaggedFormatter
      logger.extend(self)
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
