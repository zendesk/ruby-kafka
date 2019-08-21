# Basic implementation of a tagged logger that matches the API of
# ActiveSupport::TaggedLogging.

require 'delegate'
require 'logger'

module Kafka
  class TaggedLogger < SimpleDelegator

    %i(debug info warn error).each do |method|
      define_method method do |msg_or_progname, &block|
        if block_given?
          super(msg_or_progname, &block)
        else
          super("#{tags_text}#{msg_or_progname}")
        end
      end
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

    def self.new(logger_or_stream = nil)
      # don't keep wrapping the same logger over and over again
      return logger_or_stream if logger_or_stream.is_a?(TaggedLogger)
      super
    end

    def initialize(logger_or_stream = nil)
      logger = if %w(info debug warn error).all? { |s| logger_or_stream.respond_to?(s) }
        logger_or_stream
      elsif logger_or_stream
        ::Logger.new(logger_or_stream)
      else
        ::Logger.new(nil)
      end
      super(logger)
    end

    def flush
      clear_tags!
      super if defined?(super)
    end
  end

end
