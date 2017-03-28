module Kafka
  class ConnectionBuilder
    def initialize(client_id:, logger:, instrumenter:, connect_timeout:, socket_timeout:, ssl_context:, principal:, keytab:)
      @client_id = client_id
      @logger = logger
      @instrumenter = instrumenter
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @ssl_context = ssl_context
      @principal = principal
      @keytab = keytab
    end

    def build_connection(host, port)
      Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
        instrumenter: @instrumenter,
        ssl_context: @ssl_context,
        principal: @principal,
        keytab: @keytab
      )
    end
  end
end
