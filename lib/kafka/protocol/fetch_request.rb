module Kafka
  module Protocol

    # A request to fetch messages from a given partition.
    #
    # ## API Specification
    #
    #     FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
    #       ReplicaId => int32
    #       MaxWaitTime => int32
    #       MinBytes => int32
    #       TopicName => string
    #       Partition => int32
    #       FetchOffset => int64
    #       MaxBytes => int32
    #
    class FetchRequest

      # @param max_wait_time [Integer]
      # @param min_bytes [Integer]
      # @param topics [Hash]
      def initialize(max_wait_time:, min_bytes:, topics:)
        @replica_id = REPLICA_ID
        @max_wait_time = max_wait_time
        @min_bytes = min_bytes
        @topics = topics
      end

      def api_key
        FETCH_API
      end

      def api_version
        2
      end

      def response_class
        Protocol::FetchResponse
      end

      def encode(encoder)
        encoder.write_int32(@replica_id)
        encoder.write_int32(@max_wait_time)
        encoder.write_int32(@min_bytes)

        encoder.write_array(@topics) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition, config|
            fetch_offset = config.fetch(:fetch_offset)
            max_bytes = config.fetch(:max_bytes)

            encoder.write_int32(partition)
            encoder.write_int64(fetch_offset)
            encoder.write_int32(max_bytes)
          end
        end
      end
    end
  end
end
