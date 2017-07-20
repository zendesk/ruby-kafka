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
        attr_reader :partition, :error_code, :offset, :timestamp

        def initialize(partition:, error_code:, offset:, timestamp:)
          @partition = partition
          @error_code = error_code
          @offset = offset
          @timestamp = timestamp
        end
      end

      attr_reader :topics, :throttle_time_ms

      def initialize(topics: [], throttle_time_ms: 0)
        @topics = topics
        @throttle_time_ms = throttle_time_ms
      end

      def each_partition
        @topics.each do |topic_info|
          topic_info.partitions.each do |partition_info|
            yield topic_info, partition_info
          end
        end
      end

      def self.decode(decoder)
        topics = decoder.array do
          topic = decoder.string

          partitions = decoder.array do
            PartitionInfo.new(
              partition: decoder.int32,
              error_code: decoder.int16,
              offset: decoder.int64,
              timestamp: Time.at(decoder.int64/1000.0),
            )
          end

          TopicInfo.new(topic: topic, partitions: partitions)
        end

        throttle_time_ms = decoder.int32

        new(topics: topics, throttle_time_ms: throttle_time_ms)
      end
    end
  end
end
