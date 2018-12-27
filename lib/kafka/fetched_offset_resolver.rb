# frozen_string_literal: true

module Kafka
  class FetchedOffsetResolver
    def initialize(logger:)
      @logger = TaggedLogger.new(logger)
    end

    def resolve!(broker, topics)
      pending_topics = filter_pending_topics(topics)
      return topics if pending_topics.empty?

      response = broker.list_offsets(topics: pending_topics)

      pending_topics.each do |topic, partitions|
        partitions.each do |options|
          partition = options.fetch(:partition)
          resolved_offset = response.offset_for(topic, partition)

          @logger.debug "Offset for #{topic}/#{partition} is #{resolved_offset.inspect}"

          topics[topic][partition][:fetch_offset] = resolved_offset || 0
        end
      end
    end

    private

    def filter_pending_topics(topics)
      pending_topics = {}
      topics.each do |topic, partitions|
        partitions.each do |partition, options|
          offset = options.fetch(:fetch_offset)
          next if offset >= 0

          @logger.debug "Resolving offset `#{offset}` for #{topic}/#{partition}..."

          pending_topics[topic] ||= []
          pending_topics[topic] << {
            partition: partition,
            time: offset
          }
        end
      end
      pending_topics
    end
  end
end
