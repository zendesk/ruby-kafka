require "kafka/fetched_batch"

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
    def initialize(cluster:, logger:, min_bytes: 1, max_wait_time: 5)
      @cluster = cluster
      @logger = logger
      @min_bytes = min_bytes
      @max_wait_time = max_wait_time
      @topics = {}
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

      @topics.each do |topic, partitions|
        partitions.each do |partition, options|
          broker = @cluster.get_leader(topic, partition)

          topics_by_broker[broker] ||= {}
          topics_by_broker[broker][topic] ||= {}
          topics_by_broker[broker][topic][partition] = options
        end
      end

      topics_by_broker.flat_map {|broker, topics|
        resolve_offsets(broker, topics)

        options = {
          max_wait_time: @max_wait_time * 1000, # Kafka expects ms, not secs
          min_bytes: @min_bytes,
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

              raise e
            rescue Kafka::Error => e
              topic = fetched_topic.name
              partition = fetched_partition.partition
              @logger.error "Failed to fetch from #{topic}/#{partition}: #{e.message}"
              raise e
            end

            messages = fetched_partition.messages.map {|message|
              FetchedMessage.new(
                value: message.value,
                key: message.key,
                topic: fetched_topic.name,
                partition: fetched_partition.partition,
                offset: message.offset,
                create_time: message.create_time,
              )
            }

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

    def resolve_offsets(broker, topics)
      pending_topics = {}

      topics.each do |topic, partitions|
        partitions.each do |partition, options|
          offset = options.fetch(:fetch_offset)
          next if offset >= 0

          @logger.debug "Resolving offset `#{offset}` for #{topic}/#{partition}..."

          pending_topics[topic] ||= []
          pending_topics[topic] << {
            partition: partition,
            time: offset,
            max_offsets: 1,
          }
        end
      end

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
  end
end
