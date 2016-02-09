module Kafka
  module Protocol

    # A response to a list offset request.
    #
    # ## API Specification
    #
    #     OffsetResponse => [TopicName [PartitionOffsets]]
    #       PartitionOffsets => Partition ErrorCode [Offset]
    #       Partition => int32
    #       ErrorCode => int16
    #       Offset => int64
    #
    class ListOffsetResponse
      class TopicOffsetInfo
        attr_reader :name, :partition_offsets

        def initialize(name:, partition_offsets:)
          @name = name
          @partition_offsets = partition_offsets
        end
      end

      class PartitionOffsetInfo
        attr_reader :partition, :error_code, :offsets

        def initialize(partition:, error_code:, offsets:)
          @partition = partition
          @error_code = error_code
          @offsets = offsets
        end
      end

      attr_reader :topics

      def initialize(topics:)
        @topics = topics
      end

      def offset_for(topic, partition)
        topic_info = @topics.find {|t| t.name == topic }

        if topic_info.nil?
          raise UnknownTopicOrPartition, "Unknown topic #{topic}"
        end

        partition_info = topic_info
          .partition_offsets
          .find {|p| p.partition == partition }

        if partition_info.nil?
          raise UnknownTopicOrPartition, "Unknown partition #{topic}/#{partition}"
        end

        Protocol.handle_error(partition_info.error_code)

        partition_info.offsets.first
      end

      def self.decode(decoder)
        topics = decoder.array do
          name = decoder.string

          partition_offsets = decoder.array do
            PartitionOffsetInfo.new(
              partition: decoder.int32,
              error_code: decoder.int16,
              offsets: decoder.array { decoder.int64 },
            )
          end

          TopicOffsetInfo.new(
            name: name,
            partition_offsets: partition_offsets
          )
        end

        new(topics: topics)
      end
    end
  end
end
