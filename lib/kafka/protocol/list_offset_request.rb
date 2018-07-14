# frozen_string_literal: true

module Kafka
  module Protocol
    # A request to list the available offsets for a set of topics/partitions.
    #
    # ## API Specification
    #
    #     OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
    #       ReplicaId => int32
    #       IsolationLevel => int8
    #       TopicName => string
    #       Partition => int32
    #       Time => int64
    #
    class ListOffsetRequest
      ISOLATION_READ_UNCOMMITTED = 0
      ISOLATION_READ_COMMITTED = 1

      # @param topics [Hash]
      def initialize(topics:)
        @replica_id = REPLICA_ID
        @topics = topics
      end

      def api_version
        2
      end

      def api_key
        LIST_OFFSET_API
      end

      def response_class
        Protocol::ListOffsetResponse
      end

      def encode(encoder)
        encoder.write_int32(@replica_id)
        encoder.write_int8(ISOLATION_READ_COMMITTED)

        encoder.write_array(@topics) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition|
            encoder.write_int32(partition.fetch(:partition))
            encoder.write_int64(partition.fetch(:time))
          end
        end
      end
    end
  end
end
