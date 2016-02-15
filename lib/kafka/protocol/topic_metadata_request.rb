module Kafka
  module Protocol
    class TopicMetadataRequest

      # A request for cluster metadata.
      #
      # @param topics [Array<String>]
      def initialize(topics: [])
        @topics = topics
      end

      def api_key
        3
      end

      def encode(encoder)
        encoder.write_array(@topics) {|topic| encoder.write_string(topic) }
      end
    end
  end
end
