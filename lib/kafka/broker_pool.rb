require "kafka/broker"

module Kafka

  # A broker pool represents the set of brokers in a cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the pool can connect
  # to will be asked for the cluster metadata, which allows the pool to map topic
  # partitions to the current leader for those partitions.
  class BrokerPool

    # Initializes a broker pool with a set of seed brokers.
    #
    # The pool will try to fetch cluster metadata from one of the brokers.
    #
    # @param seed_brokers [Array<String>]
    # @param client_id [String]
    # @param logger [Logger]
    # @param socket_timeout [Integer, nil] see {Connection#initialize}.
    def initialize(seed_brokers:, client_id:, logger:, socket_timeout: nil)
      @client_id = client_id
      @logger = logger
      @socket_timeout = socket_timeout
      @brokers = {}
      @seed_brokers = seed_brokers
      @cluster_info = nil
    end

    def mark_as_stale!
      @cluster_info = nil
    end

    # Finds the broker acting as the leader of the given topic and partition.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @return [Integer] the broker id.
    def get_leader_id(topic, partition)
      cluster_info.find_leader_id(topic, partition)
    end

    def get_broker(broker_id)
      @brokers[broker_id] ||= connect_to_broker(broker_id)
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

    def cluster_info
      @cluster_info ||= fetch_cluster_info
    end

    # Fetches the cluster metadata.
    #
    # This is used to update the partition leadership information, among other things.
    # The methods will go through each node listed in +seed_brokers+, connecting to the
    # first one that is available. This node will be queried for the cluster metadata.
    #
    # @raise [ConnectionError] if none of the nodes in +seed_brokers+ are available.
    # @return [Protocol::MetadataResponse] the cluster metadata.
    def fetch_cluster_info
      @seed_brokers.each do |node|
        @logger.info "Trying to initialize broker pool from node #{node}"

        begin
          host, port = node.split(":", 2)

          broker = Broker.connect(
            host: host,
            port: port.to_i,
            client_id: @client_id,
            socket_timeout: @socket_timeout,
            logger: @logger,
          )

          cluster_info = broker.fetch_metadata

          @logger.info "Initialized broker pool with brokers: #{cluster_info.brokers.inspect}"

          return cluster_info
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
        ensure
          broker.disconnect unless broker.nil?
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{@seed_brokers.inspect}"
    end

    def connect_to_broker(broker_id)
      broker_info = cluster_info.find_broker(broker_id)

      Broker.connect(
        host: broker_info.host,
        port: broker_info.port,
        node_id: broker_info.node_id,
        client_id: @client_id,
        socket_timeout: @socket_timeout,
        logger: @logger,
      )
    end
  end
end
