module Kafka
  module Protocol

    # A response to a {TopicMetadataRequest}.
    #
    # The response contains information on the brokers, topics, and partitions in
    # the cluster.
    #
    # * For each broker a node id, host, and port is provided.
    # * For each topic partition the node id of the broker acting as partition leader,
    #   as well as a list of node ids for the set of replicas, are given. The `isr` list is
    #   the subset of replicas that are "in sync", i.e. have fully caught up with the
    #   leader.
    #
    # == API Specification
    #
    #     MetadataResponse => [Broker][TopicMetadata]
    #       Broker => NodeId Host Port  (any number of brokers may be returned)
    #         NodeId => int32
    #         Host => string
    #         Port => int32
    #
    #       TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    #         TopicErrorCode => int16
    #
    #       PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    #         PartitionErrorCode => int16
    #         PartitionId => int32
    #         Leader => int32
    #         Replicas => [int32]
    #         Isr => [int32]
    #
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
        # @return [String] the name of the topic
        attr_reader :topic_name

        # @return [Array<PartitionMetadata>] the partitions in the topic.
        attr_reader :partitions

        def initialize(topic_error_code:, topic_name:, partitions:)
          @topic_error_code = topic_error_code
          @topic_name = topic_name
          @partitions = partitions
        end
      end

      # @return [Array<Broker>] the list of brokers in the cluster.
      attr_reader :brokers

      # @return [Array<TopicMetadata>] the list of topics in the cluster.
      attr_reader :topics

      # Decodes a MetadataResponse from a {Decoder} containing response data.
      #
      # @param decoder [Decoder]
      # @return [nil]
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

        nil
      end
    end
  end
end
