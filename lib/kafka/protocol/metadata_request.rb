# frozen_string_literal: true

module Kafka
  module Protocol
    class MetadataRequest

      # A request for cluster metadata.
      #
      # @param topics [Array<String>]
      def initialize(topics: [])
        @topics = topics
      end

      def api_key
        TOPIC_METADATA_API
      end

      def api_version
        1
      end

      def response_class
        Protocol::MetadataResponse
      end

      def encode(encoder)
        encoder.write_array(@topics) {|topic| encoder.write_string(topic) }
      end
    end
  end
end
