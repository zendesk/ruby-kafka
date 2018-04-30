# frozen_string_literal: true

module Kafka
  module Protocol
    class OffsetCommitRequest
      # This value signals to the broker that its default configuration should be used.
      DEFAULT_RETENTION_TIME = -1

      def api_key
        OFFSET_COMMIT_API
      end

      def api_version
        2
      end

      def response_class
        OffsetCommitResponse
      end

      def initialize(group_id:, generation_id:, member_id:, retention_time: DEFAULT_RETENTION_TIME, offsets:)
        @group_id = group_id
        @generation_id = generation_id
        @member_id = member_id
        @retention_time = retention_time
        @offsets = offsets
      end

      def encode(encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@generation_id)
        encoder.write_string(@member_id)
        encoder.write_int64(@retention_time)

        encoder.write_array(@offsets) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition, offset|
            encoder.write_int32(partition)
            encoder.write_int64(offset)
            encoder.write_string(nil) # metadata
          end
        end
      end
    end
  end
end
