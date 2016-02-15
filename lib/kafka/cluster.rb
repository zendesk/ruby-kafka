require "set"
require "kafka/connection_pool"
require "kafka/protocol/topic_metadata_request"

module Kafka

  # A cluster represents the state of a Kafka cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the cluster can connect
  # to will be asked for the cluster metadata, which allows the cluster to map topic
  # partitions to the current leader for those partitions.
  class Cluster

    # Initializes a Cluster with a set of seed brokers.
    #
    # The cluster will try to fetch cluster metadata from one of the brokers.
    #
    # @param seed_brokers [Array<String>]
    # @param connection_pool [Kafka::ConnectionPool]
    # @param logger [Logger]
    def initialize(seed_brokers:, connection_pool:, logger:)
      if seed_brokers.empty?
        raise ArgumentError, "At least one seed broker must be configured"
      end

      @logger = logger
      @seed_brokers = seed_brokers
      @connection_pool = connection_pool
      @cluster_info = nil
      @stale = true

      # This is the set of topics we need metadata for. If empty, metadata for
      # all topics will be fetched.
      @target_topics = Set.new
    end

    def add_target_topics(topics)
      new_topics = Set.new(topics) - @target_topics

      unless new_topics.empty?
        @logger.info "New topics added to target list: #{new_topics.to_a.join(', ')}"

        @target_topics.merge(new_topics)

        refresh_metadata!
      end
    end

    def mark_as_stale!
      @stale = true
    end

    def refresh_metadata!
      @cluster_info = nil
      cluster_info
    end

    def refresh_metadata_if_necessary!
      refresh_metadata! if @stale
    end

    # Finds the broker acting as the leader of the given topic and partition.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @return [Kafka::Connection] connection to the broker that's currently leader.
    def get_leader(topic, partition)
      connect_to_broker(get_leader_id(topic, partition))
    end

    def partitions_for(topic)
      cluster_info.partitions_for(topic)
    end

    def topics
      cluster_info.topics.map(&:topic_name)
    end

    def disconnect
      @connection_pool.close
    end

    private

    def get_leader_id(topic, partition)
      cluster_info.find_leader_id(topic, partition)
    end

    def cluster_info
      @cluster_info ||= fetch_cluster_info
    end

    # Fetches the cluster metadata.
    #
    # This is used to update the partition leadership information, among other things.
    # The methods will go through each node listed in `seed_brokers`, connecting to the
    # first one that is available. This node will be queried for the cluster metadata.
    #
    # @raise [ConnectionError] if none of the nodes in `seed_brokers` are available.
    # @return [Protocol::MetadataResponse] the cluster metadata.
    def fetch_cluster_info
      @seed_brokers.each do |node|
        @logger.info "Fetching cluster metadata from #{node}"

        begin
          host, port = node.split(":", 2)

          connection = @connection_pool.connect(host, port.to_i)
          request = Protocol::TopicMetadataRequest.new(topics: @target_topics)
          cluster_info = connection.send_request(request)

          @stale = false

          @logger.info "Discovered cluster metadata; nodes: #{cluster_info.brokers.join(', ')}"

          return cluster_info
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{@seed_brokers.join(', ')}"
    end

    def connect_to_broker(broker_id)
      info = cluster_info.find_broker(broker_id)

      @connection_pool.connect(info.host, info.port)
    end
  end
end
