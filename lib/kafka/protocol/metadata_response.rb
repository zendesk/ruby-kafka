module Kafka
  module Protocol
    class MetadataResponse
      class Broker
        attr_reader :node_id, :host, :port

        def initialize(node_id:, host:, port:)
          @node_id = node_id
          @host = host
          @port = port
        end
      end

      class PartitionMetadata
        def initialize(partition_error_code:, partition_id:, leader:, replicas:, isr:)
          @partition_error_code = partition_error_code
          @partition_id = partition_id
          @leader = leader
          @replicas = replicas
          @isr = isr
        end
      end

      class TopicMetadata
        def initialize(topic_error_code:, topic_name:, partitions:)
          @topic_error_code = topic_error_code
          @topic_name = topic_name
          @partitions = partitions
        end
      end

      attr_reader :brokers, :topics

      def decode(decoder)
        @brokers = decoder.array do
          node_id = decoder.int32
          host = decoder.string
          port = decoder.int32

          Broker.new(
            node_id: node_id,
            host: host,
            port: port
          )
        end

        @topics = decoder.array do
          topic_error_code = decoder.int16
          topic_name = decoder.string

          partitions = decoder.array do
            PartitionMetadata.new(
              partition_error_code: decoder.int16,
              partition_id: decoder.int32,
              leader: decoder.int32,
              replicas: decoder.array { decoder.int32 },
              isr: decoder.array { decoder.int32 },
            )
          end

          TopicMetadata.new(
            topic_error_code: topic_error_code,
            topic_name: topic_name,
            partitions: partitions,
          )
        end
      end
    end
  end
end
