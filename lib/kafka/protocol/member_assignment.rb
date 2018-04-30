# frozen_string_literal: true

module Kafka
  module Protocol
    class MemberAssignment
      attr_reader :topics

      def initialize(version: 0, topics: {}, user_data: nil)
        @version = version
        @topics = topics
        @user_data = user_data
      end

      def assign(topic, partitions)
        @topics[topic] ||= []
        @topics[topic].concat(partitions)
      end

      def encode(encoder)
        encoder.write_int16(@version)

        encoder.write_array(@topics) do |topic, partitions|
          encoder.write_string(topic)

          encoder.write_array(partitions) do |partition|
            encoder.write_int32(partition)
          end
        end

        encoder.write_bytes(@user_data)
      end

      def self.decode(decoder)
        new(
          version: decoder.int16,
          topics: Hash[decoder.array { [decoder.string, decoder.array { decoder.int32 }] }],
          user_data: decoder.bytes,
        )
      end
    end
  end
end
