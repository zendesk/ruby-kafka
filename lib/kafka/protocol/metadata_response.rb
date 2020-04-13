# frozen_string_literal: true

module Kafka
  module Protocol

    # A response to a {MetadataRequest}.
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
    # ## API Specification
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
      class PartitionMetadata
        attr_reader :partition_id, :leader, :replicas

        attr_reader :partition_error_code

        def initialize(partition_error_code:, partition_id:, leader:, replicas: [], isr: [])
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

        attr_reader :topic_error_code

        def initialize(topic_error_code: 0, topic_name:, partitions:)
          @topic_error_code = topic_error_code
          @topic_name = topic_name
          @partitions = partitions
        end
      end

      # @return [Array<Kafka::BrokerInfo>] the list of brokers in the cluster.
      attr_reader :brokers

      # @return [Array<TopicMetadata>] the list of topics in the cluster.
      attr_reader :topics

      # @return [Integer] The broker id of the controller broker.
      attr_reader :controller_id

      def initialize(brokers:, controller_id:, topics:)
        @brokers = brokers
        @controller_id = controller_id
        @topics = topics
      end

      # Finds the node id of the broker that is acting as leader for the given topic
      # and partition per this metadata.
      #
      # @param topic [String] the name of the topic.
      # @param partition [Integer] the partition number.
      # @return [Integer] the node id of the leader.
      def find_leader_id(topic, partition)
        topic_info = @topics.find {|t| t.topic_name == topic }

        if topic_info.nil?
          raise UnknownTopicOrPartition, "no topic #{topic}"
        end

        Protocol.handle_error(topic_info.topic_error_code)

        partition_info = topic_info.partitions.find {|p| p.partition_id == partition }

        if partition_info.nil?
          raise UnknownTopicOrPartition, "no partition #{partition} in topic #{topic}"
        end

        begin
          Protocol.handle_error(partition_info.partition_error_code)
        rescue ReplicaNotAvailable
          # This error can be safely ignored per the protocol specification.
        end

        partition_info.leader
      end

      # Finds the broker info for the given node id.
      #
      # @param node_id [Integer] the node id of the broker.
      # @return [Kafka::BrokerInfo] information about the broker.
      def find_broker(node_id)
        broker = @brokers.find {|b| b.node_id == node_id }

        raise Kafka::NoSuchBroker, "No broker with id #{node_id}" if broker.nil?

        broker
      end

      def controller_broker
        find_broker(controller_id)
      end

      def partitions_for(topic_name)
        topic = @topics.find {|t| t.topic_name == topic_name }

        if topic.nil?
          raise UnknownTopicOrPartition, "unknown topic #{topic_name}"
        end

        Protocol.handle_error(topic.topic_error_code)

        topic.partitions
      end

      # Decodes a MetadataResponse from a {Decoder} containing response data.
      #
      # @param decoder [Decoder]
      # @return [MetadataResponse] the metadata response.
      def self.decode(decoder)
        brokers = decoder.array do
          node_id = decoder.int32
          host = decoder.string
          port = decoder.int32
          _rack = decoder.string

          BrokerInfo.new(
            node_id: node_id,
            host: host,
            port: port
          )
        end

        controller_id = decoder.int32

        topics = decoder.array do
          topic_error_code = decoder.int16
          topic_name = decoder.string
          _is_internal = decoder.boolean

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

        new(brokers: brokers, controller_id: controller_id, topics: topics)
      end
    end
  end
end
