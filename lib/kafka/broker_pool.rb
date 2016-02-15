require "kafka/broker"

module Kafka
  class BrokerPool
    def initialize(client_id:, connect_timeout: nil, socket_timeout: nil, logger:)
      @client_id = client_id
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @logger = logger
      @brokers = {}
    end

    def connect(host, port)
      addr = [host, port].join(":")

      @brokers[addr] ||= Broker.connect(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
      )
    end

    def close
      @brokers.each do |_, broker|
        @logger.info "Disconnecting broker #{broker}"
        broker.disconnect
      end
    end
  end
end
