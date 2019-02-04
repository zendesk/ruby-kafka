# frozen_string_literal: true

require "kafka/broker"

module Kafka
  class BrokerPool
    def initialize(connection_builder:, logger:)
      @logger = TaggedLogger.new(logger)
      @connection_builder = connection_builder
      @brokers = {}
    end

    def connect(host, port, node_id: nil)
      if @brokers.key?(node_id)
        broker = @brokers.fetch(node_id)
        return broker if broker.address_match?(host, port)
        broker.disconnect
        @brokers[node_id] = nil
      end

      broker = Broker.new(
        connection_builder: @connection_builder,
        host: host,
        port: port,
        node_id: node_id,
        logger: @logger,
      )

      @brokers[node_id] = broker unless node_id.nil?

      broker
    end

    def close
      @brokers.each do |id, broker|
        @logger.info "Disconnecting broker #{id}"
        broker.disconnect
      end
    end
  end
end
