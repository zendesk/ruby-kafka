require "kafka/broker"

module Kafka
  class BrokerPool
    def initialize(connection_builder:, logger:)
      @logger = logger
      @connection_builder = connection_builder
      @brokers = {}
    end

    def connect(host, port, node_id: nil)
      uri = [host, port].join(":")
      broker = fetch_broker(node_id, uri)

      return broker if broker

      broker = Broker.new(
        connection: @connection_builder.build_connection(host, port),
        node_id: node_id,
        logger: @logger,
      )

      set_broker(node_id, uri, broker)

      broker
    end

    def close
      @brokers.each do |id, broker_object|
        disconnect_broker(id, broker_object[:broker])
      end
    end

    private

    def fetch_broker(node_id, uri)
      return if node_id.nil? || !@brokers.key?(node_id)

      broker_object = @brokers.fetch(node_id)
      broker = broker_object[:broker]

      unless valid_cache?(broker_object, uri)
        # Disconnect from invalid stale cached broker
        disconnect_broker(node_id, broker)

        return
      end

      broker
    end

    def set_broker(node_id, uri, broker)
      return if node_id.nil?

      @brokers[node_id] = { broker: broker, uri: uri }
    end

    def valid_cache?(broker_object, uri)
      broker_object[:uri] == uri
    end

    def disconnect_broker(node_id, broker)
      @logger.info "Disconnecting broker #{node_id}"

      broker.disconnect
    end
  end
end
