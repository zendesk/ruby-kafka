require "kafka/broker"

module Kafka

  # A broker pool represents the set of brokers in a cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the pool can connect
  # to will be asked for the cluster metadata, which allows the pool to map topic
  # partitions to the current leader for those partitions.
  class BrokerPool
    def initialize(seed_brokers:, client_id:, logger:)
      @client_id = client_id
      @logger = logger
      @brokers = {}

      initialize_from_seed_brokers(seed_brokers)
    end

    def get_leader(topic, partition)
      leader_id = @cluster_info.find_leader_id(topic, partition)

      broker_for_id(leader_id)
    end

    private

    def broker_for_id(broker_id)
      @brokers[broker_id] ||= connect_to_broker(broker_id)
    end

    def connect_to_broker(broker_id)
      broker_info = @cluster_info.find_broker(broker_id)

      Broker.new(
        host: broker_info.host,
        port: broker_info.port,
        node_id: broker_info.node_id,
        client_id: @client_id,
        logger: @logger,
      )
    end

    def initialize_from_seed_brokers(seed_brokers)
      seed_brokers.each do |node|
        @logger.info "Trying to initialize broker pool from node #{node}"

        begin
          host, port = node.split(":", 2)

          broker = Broker.new(host: host, port: port, client_id: @client_id, logger: @logger)

          @cluster_info = broker.fetch_metadata

          @logger.info "Initialized broker pool with brokers: #{@cluster_info.brokers.inspect}"

          return
        rescue Error => e
          @logger.error "Failed to fetch metadata from broker #{broker}: #{e}"
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{seed_brokers.inspect}"
    end
  end
end
