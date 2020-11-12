# frozen_string_literal: true

module Kafka
  module Protocol
    class TxnOffsetCommitResponse
      class PartitionError
        attr_reader :partition, :error_code

        def initialize(partition:, error_code:)
          @partition = partition
          @error_code = error_code
        end
      end

      class TopicPartitionsError
        attr_reader :topic, :partitions

        def initialize(topic:, partitions:)
          @topic = topic
          @partitions = partitions
        end
      end

      attr_reader :errors

      def initialize(errors:)
        @errors = errors
      end

      def self.decode(decoder)
        _throttle_time_ms = decoder.int32
        errors = decoder.array do
          TopicPartitionsError.new(
            topic: decoder.string,
            partitions: decoder.array do
              PartitionError.new(
                partition: decoder.int32,
                error_code: decoder.int16
              )
            end
          )
        end
        new(errors: errors)
      end
    end
  end
end
