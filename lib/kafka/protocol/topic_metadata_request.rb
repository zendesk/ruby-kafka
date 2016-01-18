module Kafka
  module Protocol
    class TopicMetadataRequest
      def initialize(topics: [])
        @topics = topics
      end

      def encode(encoder)
        encoder.write_array(@topics) {|topic| encoder.write_string(topic) }
      end
    end
  end
end
