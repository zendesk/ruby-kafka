# frozen_string_literal: true

require "kafka/fetched_offset_resolver"
require "kafka/fetched_batch_generator"

module Kafka

  # Fetches messages from one or more partitions.
  #
  #     operation = Kafka::FetchOperation.new(
  #       cluster: cluster,
  #       logger: logger,
  #       min_bytes: 1,
  #       max_wait_time: 10,
  #     )
  #
  #     # These calls will schedule fetches from the specified topics/partitions.
  #     operation.fetch_from_partition("greetings", 42, offset: :latest, max_bytes: 100000)
  #     operation.fetch_from_partition("goodbyes", 13, offset: :latest, max_bytes: 100000)
  #
  #     operation.execute
  #
  class FetchOperation
    def initialize(cluster:, logger:, min_bytes: 1, max_bytes: 10485760, max_wait_time: 5)
      @cluster = cluster
      @logger = TaggedLogger.new(logger)
      @min_bytes = min_bytes
      @max_bytes = max_bytes
      @max_wait_time = max_wait_time
      @topics = {}

      @offset_resolver = Kafka::FetchedOffsetResolver.new(
        logger: logger
      )
    end

    def fetch_from_partition(topic, partition, offset: :latest, max_bytes: 1048576)
      if offset == :earliest
        offset = -2
      elsif offset == :latest
        offset = -1
      end

      @topics[topic] ||= {}
      @topics[topic][partition] = {
        fetch_offset: offset,
        max_bytes: max_bytes,
      }
    end

    def execute
      @cluster.add_target_topics(@topics.keys)
      @cluster.refresh_metadata_if_necessary!

      topics_by_broker = {}

      if @topics.none? {|topic, partitions| partitions.any? }
        raise NoPartitionsToFetchFrom
      end

      @topics.each do |topic, partitions|
        partitions.each do |partition, options|
          broker = @cluster.get_leader(topic, partition)

          topics_by_broker[broker] ||= {}
          topics_by_broker[broker][topic] ||= {}
          topics_by_broker[broker][topic][partition] = options
        end
      end

      topics_by_broker.flat_map do |broker, topics|
        @offset_resolver.resolve!(broker, topics)

        options = {
          max_wait_time: @max_wait_time * 1000, # Kafka expects ms, not secs
          min_bytes: @min_bytes,
          max_bytes: @max_bytes,
          topics: topics,
        }

        response = broker.fetch_messages(**options)

        response.topics.flat_map do |fetched_topic|
          fetched_topic.partitions.map do |fetched_partition|
            begin
              Protocol.handle_error(fetched_partition.error_code)
            rescue Kafka::OffsetOutOfRange => e
              e.topic = fetched_topic.name
              e.partition = fetched_partition.partition
              e.offset = topics.fetch(e.topic).fetch(e.partition).fetch(:fetch_offset)

              raise e
            rescue Kafka::Error => e
              topic = fetched_topic.name
              partition = fetched_partition.partition
              @logger.error "Failed to fetch from #{topic}/#{partition}: #{e.message}"
              raise e
            end

            Kafka::FetchedBatchGenerator.new(
              fetched_topic.name,
              fetched_partition,
              topics.fetch(fetched_topic.name).fetch(fetched_partition.partition).fetch(:fetch_offset),
              logger: @logger
            ).generate
          end
        end
      end
    rescue Kafka::ConnectionError, Kafka::LeaderNotAvailable, Kafka::NotLeaderForPartition
      @cluster.mark_as_stale!

      raise
    end
  end
end
