module Kafka
  module Protocol
    # A request to list the available offsets for a set of topics/partitions.
    #
    # ## API Specification
    #
    #     OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
    #       ReplicaId => int32
    #       TopicName => string
    #       Partition => int32
    #       Time => int64
    #       MaxNumberOfOffsets => int32
    #
    class ListOffsetRequest

      # @param topics [Hash]
      def initialize(topics:)
        @replica_id = REPLICA_ID
        @topics = topics
      end

      def api_key
        LIST_OFFSET_API
      end

      def response_class
        Protocol::ListOffsetResponse
      end

      def encode(encoder)
        encoder.write_int32(@replica_id)

        encoder.write_array(@topics) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition|
            encoder.write_int32(partition.fetch(:partition))
            encoder.write_int64(partition.fetch(:time))
            encoder.write_int32(partition.fetch(:max_offsets))
          end
        end
      end
    end
  end
end
