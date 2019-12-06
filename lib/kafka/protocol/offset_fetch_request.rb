# frozen_string_literal: true

module Kafka
  module Protocol
    class OffsetFetchRequest
      def initialize(group_id:, topics:)
        @group_id = group_id
        @topics = topics
      end

      def api_key
        OFFSET_FETCH_API
      end

      def api_version
        @topics.nil? ? 2 : 1
      end

      def response_class
        OffsetFetchResponse
      end

      def encode(encoder)
        encoder.write_string(@group_id)

        encoder.write_array(@topics) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition|
            encoder.write_int32(partition)
          end
        end
      end
    end
  end
end
