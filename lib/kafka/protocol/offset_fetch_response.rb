# frozen_string_literal: true

module Kafka
  module Protocol
    class OffsetFetchResponse
      class PartitionOffsetInfo
        attr_reader :offset, :metadata, :error_code

        def initialize(offset:, metadata:, error_code:)
          @offset = offset
          @metadata = metadata
          @error_code = error_code
        end
      end

      attr_reader :topics

      def initialize(topics:)
        @topics = topics
      end

      def offset_for(topic, partition)
        offset_info = topics.fetch(topic).fetch(partition, nil)

        if offset_info
          Protocol.handle_error(offset_info.error_code)
          offset_info.offset
        else
          -1
        end
      end

      def self.decode(decoder)
        topics = decoder.array {
          topic = decoder.string

          partitions = decoder.array {
            partition = decoder.int32

            info = PartitionOffsetInfo.new(
              offset: decoder.int64,
              metadata: decoder.string,
              error_code: decoder.int16,
            )

            [partition, info]
          }

          [topic, Hash[partitions]]
        }

        new(topics: Hash[topics])
      end
    end
  end
end
