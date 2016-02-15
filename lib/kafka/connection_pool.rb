require "kafka/connection"

module Kafka
  class ConnectionPool
    def initialize(client_id:, connect_timeout: nil, socket_timeout: nil, logger:)
      @client_id = client_id
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @logger = logger
      @connections = {}
    end

    def connect(host, port)
      addr = [host, port].join(":")

      @connections[addr] ||= Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
      )
    end

    def close
      @connections.values.each do |connection|
        @logger.info "Closing connection to #{connection}"
        connection.close
      end
    end
  end
end
