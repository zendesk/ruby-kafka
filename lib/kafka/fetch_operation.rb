# frozen_string_literal: true

require "kafka/fetched_batch"
require "kafka/fetch_offset_resolver"

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
    ABORTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x00".freeze

    def initialize(cluster:, logger:, min_bytes: 1, max_bytes: 10485760, max_wait_time: 5)
      @cluster = cluster
      @logger = logger
      @min_bytes = min_bytes
      @max_bytes = max_bytes
      @max_wait_time = max_wait_time
      @topics = {}

      @offset_resolver = Kafka::FetchOffsetResolver.new(
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

      topics_by_broker.flat_map {|broker, topics|
        @offset_resolver.resolve!(broker, topics)

        options = {
          max_wait_time: @max_wait_time * 1000, # Kafka expects ms, not secs
          min_bytes: @min_bytes,
          max_bytes: @max_bytes,
          topics: topics,
        }

        response = broker.fetch_messages(**options)

        response.topics.flat_map {|fetched_topic|
          fetched_topic.partitions.map {|fetched_partition|
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

            messages = extract_messages(fetched_topic, fetched_partition)

            FetchedBatch.new(
              topic: fetched_topic.name,
              partition: fetched_partition.partition,
              highwater_mark_offset: fetched_partition.highwater_mark_offset,
              messages: messages,
            )
          }
        }
      }
    rescue Kafka::ConnectionError, Kafka::LeaderNotAvailable, Kafka::NotLeaderForPartition
      @cluster.mark_as_stale!

      raise
    end

    private

    def extract_messages(fetched_topic, fetched_partition)
      return [] if fetched_partition.messages.empty?

      if fetched_partition.messages.first.is_a?(Kafka::Protocol::MessageSet)
        fetched_partition.messages.flat_map { |message_set| message_set.messages }
      else
        records_by_producer_id = {}
        fetched_partition.messages.each do |record_batch|
          record_batch.records.each do |record|
            if record.is_control_record
              exclude_aborted_transaction(
                record, records_by_producer_id, fetched_partition.aborted_transactions
              )
            else
              records_by_producer_id[record_batch.producer_id] ||= []
              records_by_producer_id[record_batch.producer_id] << FetchedMessage.new(
                message: record,
                topic: fetched_topic.name,
                partition: fetched_partition.partition
              )
            end
          end
        end
        records_by_producer_id.values.flatten
      end
    end

    def exclude_aborted_transaction(control_record, records_by_producer_id, aborted_transactions)
      return if control_record.key != ABORTED_TRANSACTION_SIGNAL || aborted_transactions.empty?
      aborted_transactions.each do |abort_transaction|
        if records_by_producer_id.key?(abort_transaction[:producer_id])
          records_by_producer_id[abort_transaction[:producer_id]].reject! do |record|
            record.offset >= abort_transaction[:first_offset]
          end
        end
      end
    end
  end
end
