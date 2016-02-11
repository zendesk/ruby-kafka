require "kafka/broker"
require "kafka/cluster_metadata"

module Kafka

  # A Cluster represents the set of brokers in a cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the pool can connect
  # to will be asked for the cluster metadata, which allows the pool to map topic
  # partitions to the current leader for those partitions.
  class Cluster

    # Initializes a broker pool with a set of seed brokers.
    #
    # The pool will try to fetch cluster metadata from one of the brokers.
    #
    # @param logger [Logger]
    # @param client [Kafka::BrokerClient]
    def initialize(logger:, client:, seed_brokers:)
      @logger = logger
      @client = client
      @brokers = {}

      @cluster_metadata = ClusterMetadata.new(
        client: @client,
        seed_brokers: seed_brokers,
        logger: @logger,
      )
    end

    # Finds the broker acting as the leader of the given topic and partition.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @return [Broker] the broker that's currently leader.
    def get_leader(topic, partition)
      leader_id = @cluster_metadata.get_leader_id(topic, partition)
      get_broker(leader_id)
    end

    def topics
      @cluster_metadata.topics
    end

    def shutdown
      @brokers.each do |id, broker|
        @logger.info "Disconnecting broker #{id}"
        broker.disconnect
      end
    end

    def partitions_for(topic)
      @cluster_metadata.partitions_for(topic)
    end

    def mark_as_stale!
      @cluster_metadata.mark_as_stale!
    end

    private

    def get_broker(broker_id)
      @brokers[broker_id] ||= connect_to_broker(broker_id)
    end

    def connect_to_broker(broker_id)
      broker = @cluster_metadata.find_broker(broker_id)

      @client.connect_to(broker.host, broker.port, node_id: broker.node_id)
    end
  end
end
