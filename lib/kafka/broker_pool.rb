require "kafka/broker"

module Kafka

  # A broker pool represents the set of brokers in a cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the pool can connect
  # to will be asked for the cluster metadata, which allows the pool to map topic
  # partitions to the current leader for those partitions.
  class BrokerPool

    # The number of times to try to connect to a broker before giving up.
    MAX_CONNECTION_ATTEMPTS = 3

    # The backoff period between connection retries, in seconds.
    RETRY_BACKOFF_TIMEOUT = 5

    # Initializes a broker pool with a set of seed brokers.
    #
    # The pool will try to fetch cluster metadata from one of the brokers.
    #
    # @param seed_brokers [Array<String>]
    # @param client_id [String]
    # @param logger [Logger]
    def initialize(seed_brokers:, client_id:, logger:, socket_timeout: nil)
      @client_id = client_id
      @logger = logger
      @socket_timeout = socket_timeout
      @brokers = {}
      @seed_brokers = seed_brokers

      refresh
    end

    # Refreshes the cluster metadata.
    #
    # This is used to update the partition leadership information, among other things.
    # The methods will go through each node listed in `seed_brokers`, connecting to the
    # first one that is available. This node will be queried for the cluster metadata.
    #
    # @raise [ConnectionError] if none of the nodes in `seed_brokers` are available.
    # @return [nil]
    def refresh
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

          @cluster_info = broker.fetch_metadata

          @logger.info "Initialized broker pool with brokers: #{@cluster_info.brokers.inspect}"

          return
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{@seed_brokers.inspect}"
    end

    # Finds the broker acting as the leader of the given topic and partition and connects to it.
    #
    # Note that this call may take a considerable amount of time, since the cached cluster
    # metadata may be out of date. In that case, the cluster needs to be re-discovered. This
    # can happen when a broker becomes unavailable, which would trigger a leader election for
    # the partitions previously owned by that broker. Since this can take some time, this method
    # will retry up to `MAX_CONNECTION_ATTEMPTS` times, waiting `RETRY_BACKOFF_TIMEOUT` seconds
    # between each attempt.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @raise [ConnectionError] if it was not possible to connect to the leader.
    # @return [Broker] the broker that's currently acting as leader of the partition.
    def get_leader(topic, partition)
      attempt = 0

      begin
        leader_id = @cluster_info.find_leader_id(topic, partition)
        broker_for_id(leader_id)
      rescue ConnectionError => e
        @logger.error "Failed to connect to leader for topic `#{topic}`, partition #{partition}"

        if attempt < MAX_CONNECTION_ATTEMPTS
          attempt += 1

          @logger.info "Rediscovering cluster and retrying in #{RETRY_BACKOFF_TIMEOUT} second(s)"

          sleep RETRY_BACKOFF_TIMEOUT
          refresh
          retry
        else
          @logger.error "Giving up trying to find leader for topic `#{topic}`, partition #{partition}"

          raise e
        end
      end
    end

    def partitions_for(topic)
      @cluster_info.partitions_for(topic)
    end

    def shutdown
      @brokers.each do |id, broker|
        @logger.info "Disconnecting broker #{id}"
        broker.disconnect
      end
    end

    private

    def broker_for_id(broker_id)
      @brokers[broker_id] ||= connect_to_broker(broker_id)
    end

    def connect_to_broker(broker_id)
      broker_info = @cluster_info.find_broker(broker_id)

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
