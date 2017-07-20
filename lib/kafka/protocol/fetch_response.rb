require "kafka/protocol/message_set"

module Kafka
  module Protocol

    # A response to a fetch request.
    #
    # ## API Specification
    #
    #     FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
    #       TopicName => string
    #       Partition => int32
    #       ErrorCode => int16
    #       HighwaterMarkOffset => int64
    #       MessageSetSize => int32
    #
    class FetchResponse
      class FetchedPartition
        attr_reader :partition, :error_code
        attr_reader :highwater_mark_offset, :messages

        def initialize(partition:, error_code:, highwater_mark_offset:, messages:)
          @partition = partition
          @error_code = error_code
          @highwater_mark_offset = highwater_mark_offset
          @messages = messages
        end
      end

      class FetchedTopic
        attr_reader :name, :partitions

        def initialize(name:, partitions:)
          @name = name
          @partitions = partitions
        end
      end

      attr_reader :topics

      def initialize(topics: [], throttle_time_ms: 0)
        @topics = topics
        @throttle_time_ms = throttle_time_ms
      end

      def self.decode(decoder)
        throttle_time_ms = decoder.int32

        topics = decoder.array do
          topic_name = decoder.string

          partitions = decoder.array do
            partition = decoder.int32
            error_code = decoder.int16
            highwater_mark_offset = decoder.int64

            message_set_decoder = Decoder.from_string(decoder.bytes)
            message_set = MessageSet.decode(message_set_decoder)

            FetchedPartition.new(
              partition: partition,
              error_code: error_code,
              highwater_mark_offset: highwater_mark_offset,
              messages: message_set.messages,
            )
          end

          FetchedTopic.new(
            name: topic_name,
            partitions: partitions,
          )
        end

        new(topics: topics, throttle_time_ms: throttle_time_ms)
      end
    end
  end
end
