require "kafka/broker_pool"
require "kafka/producer"

module Kafka
  class Client
    def initialize(seed_brokers:, client_id:, logger:)
      @seed_brokers = seed_brokers
      @client_id = client_id
      @logger = logger
    end

    def get_producer(**options)
      broker_pool = BrokerPool.new(
        seed_brokers: @seed_brokers,
        client_id: @client_id,
        logger: @logger
      )

      Producer.new(broker_pool: broker_pool, logger: @logger, **options)
    end
  end
end
