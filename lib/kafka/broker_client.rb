module Kafka
  class BrokerClient
    # @param client_id [String]
    # @param logger [Logger]
    # @param connect_timeout [Integer, nil] see {Connection#initialize}.
    # @param socket_timeout [Integer, nil] see {Connection#initialize}.
    def initialize(client_id:, logger:, connect_timeout: nil, socket_timeout: nil)
      @client_id = client_id
      @logger = logger
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
    end

    # @param host [String]
    # @param port [Integer]
    # @param node_id [Integer]
    # @return [Kafka::Broker]
    def connect_to(host, port, node_id: nil)
      connection = Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
      )

      Broker.new(
        connection: connection,
        node_id: node_id,
        logger: @logger,
      )
    end
  end
end
