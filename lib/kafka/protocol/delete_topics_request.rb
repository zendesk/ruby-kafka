# frozen_string_literal: true

module Kafka
  module Protocol

    class DeleteTopicsRequest
      def initialize(topics:, timeout:)
        @topics, @timeout = topics, timeout
      end

      def api_key
        DELETE_TOPICS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::DeleteTopicsResponse
      end

      def encode(encoder)
        encoder.write_array(@topics) do |topic|
          encoder.write_string(topic)
        end
        # Timeout is in ms.
        encoder.write_int32(@timeout * 1000)
      end
    end

  end
end
