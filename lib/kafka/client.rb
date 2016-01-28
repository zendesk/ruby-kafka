require "kafka/broker_pool"
require "kafka/producer"

module Kafka
  class Client

    # Initializes a new Kafka client.
    #
    # @param seed_brokers [Array<String>] the list of brokers used to initialize
    #   the client.
    #
    # @param client_id [String] the identifier for this application.
    #
    # @param logger [Logger]
    #
    # @param socket_timeout [Integer, nil] the timeout setting for socket
    #   connections. See {BrokerPool#initialize}.
    #
    # @return [Client]
    def initialize(seed_brokers:, client_id:, logger:, socket_timeout: nil)
      @seed_brokers = seed_brokers
      @client_id = client_id
      @logger = logger
      @socket_timeout = socket_timeout
    end

    def get_producer(**options)
      broker_pool = BrokerPool.new(
        seed_brokers: @seed_brokers,
        client_id: @client_id,
        logger: @logger,
        socket_timeout: @socket_timeout,
      )

      Producer.new(broker_pool: broker_pool, logger: @logger, **options)
    end
  end
end
