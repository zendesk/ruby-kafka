require "set"
require "kafka/broker"

module Kafka

  # A broker cluster represents the set of brokers in a cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the cluster can connect
  # to will be asked for the cluster metadata, which allows the cluster to map topic
  # partitions to the current leader for those partitions.
  class Cluster

    # Initializes a broker cluster with a set of seed brokers.
    #
    # The cluster will try to fetch cluster metadata from one of the brokers.
    #
    # @param seed_brokers [Array<String>]
    # @param client_id [String]
    # @param logger [Logger]
    # @param connect_timeout [Integer, nil] see {Connection#initialize}.
    # @param socket_timeout [Integer, nil] see {Connection#initialize}.
    def initialize(seed_brokers:, client_id:, logger:, connect_timeout: nil, socket_timeout: nil)
      if seed_brokers.empty?
        raise ArgumentError, "At least one seed broker must be configured"
      end

      @client_id = client_id
      @logger = logger
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @brokers = {}
      @seed_brokers = seed_brokers
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
    # @return [Broker] the broker that's currently leader.
    def get_leader(topic, partition)
      get_broker(get_leader_id(topic, partition))
    end

    def partitions_for(topic)
      cluster_info.partitions_for(topic)
    end

    def topics
      cluster_info.topics.map(&:topic_name)
    end

    def shutdown
      @brokers.each do |id, broker|
        @logger.info "Disconnecting broker #{id}"
        broker.disconnect
      end
    end

    private

    def get_leader_id(topic, partition)
      cluster_info.find_leader_id(topic, partition)
    end

    def get_broker(broker_id)
      @brokers[broker_id] ||= connect_to_broker(broker_id)
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

          broker = Broker.connect(
            host: host,
            port: port.to_i,
            client_id: @client_id,
            socket_timeout: @socket_timeout,
            logger: @logger,
          )

          cluster_info = broker.fetch_metadata(topics: @target_topics)

          @stale = false

          @logger.info "Discovered cluster metadata; nodes: #{cluster_info.brokers.join(', ')}"

          return cluster_info
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
        ensure
          broker.disconnect unless broker.nil?
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{@seed_brokers.join(', ')}"
    end

    def connect_to_broker(broker_id)
      broker_info = cluster_info.find_broker(broker_id)

      Broker.connect(
        host: broker_info.host,
        port: broker_info.port,
        node_id: broker_info.node_id,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
      )
    end
  end
end
