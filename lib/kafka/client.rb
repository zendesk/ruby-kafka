require "kafka/broker_pool"
require "kafka/producer"

module Kafka
  class Client
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
