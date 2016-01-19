module Kafka
  module Protocol
    class ProduceResponse
      class TopicInfo
        attr_reader :topic, :partitions

        def initialize(topic:, partitions:)
          @topic = topic
          @partitions = partitions
        end
      end

      class PartitionInfo
        attr_reader :partition, :error_code, :offset

        def initialize(partition:, error_code:, offset:)
          @partition = partition
          @error_code = error_code
          @offset = offset
        end
      end

      attr_reader :topics

      def initialize(topics: [])
        @topics = topics
      end

      def decode(decoder)
        @topics = decoder.array do
          topic = decoder.string

          partitions = decoder.array do
            PartitionInfo.new(
              partition: decoder.int32,
              error_code: decoder.int16,
              offset: decoder.int64,
            )
          end

          TopicInfo.new(topic: topic, partitions: partitions)
        end
      end
    end
  end
end
