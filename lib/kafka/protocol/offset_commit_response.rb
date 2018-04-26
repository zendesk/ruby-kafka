# frozen_string_literal: true

module Kafka
  module Protocol
    class OffsetCommitResponse
      attr_reader :topics

      def initialize(topics:)
        @topics = topics
      end

      def self.decode(decoder)
        topics = decoder.array {
          topic = decoder.string
          partitions = decoder.array {
            partition = decoder.int32
            error_code = decoder.int16

            [partition, error_code]
          }

          [topic, Hash[partitions]]
        }

        new(topics: Hash[topics])
      end
    end
  end
end
