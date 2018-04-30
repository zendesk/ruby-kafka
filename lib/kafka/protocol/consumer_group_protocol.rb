# frozen_string_literal: true

module Kafka
  module Protocol
    class ConsumerGroupProtocol
      def initialize(version: 0, topics:, user_data: nil)
        @version = version
        @topics = topics
        @user_data = user_data
      end

      def encode(encoder)
        encoder.write_int16(@version)
        encoder.write_array(@topics) {|topic| encoder.write_string(topic) }
        encoder.write_bytes(@user_data)
      end
    end
  end
end
