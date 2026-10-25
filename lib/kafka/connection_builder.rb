# frozen_string_literal: true

require "kafka/reconnect_backoff"

module Kafka
  class ConnectionBuilder
    def initialize(client_id:, logger:, instrumenter:, connect_timeout:, socket_timeout:, ssl_context:, sasl_authenticator:,
                   reconnect_backoff: 0, reconnect_backoff_max: 1)
      @reconnect_backoff = ReconnectBackoff.new(reconnect_backoff, reconnect_backoff_max)
      @client_id = client_id
      @logger = TaggedLogger.new(logger)
      @instrumenter = instrumenter
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @ssl_context = ssl_context
      @sasl_authenticator = sasl_authenticator
    end

    def with_connection(host, port, existing_connection)
      @reconnect_backoff.with_attempt(host, port, existing_connection) do
        yield(existing_connection || build_connection(host, port))
      end
    end

    def build_connection(host, port)
      connection = Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
        instrumenter: @instrumenter,
        ssl_context: @ssl_context,
      )

      @sasl_authenticator.authenticate!(connection)

      connection
    end

  end
end
