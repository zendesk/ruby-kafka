require "stringio"
require "kafka/socket_with_timeout"
require "kafka/ssl_socket_with_timeout"
require "kafka/protocol/request_message"
require "kafka/protocol/encoder"
require "kafka/protocol/decoder"
require "kafka/connection_operation"
require "kafka/sasl_socket_with_timeout"

module Kafka

  # A connection to a single Kafka broker.
  #
  # Usually you'll need a separate connection to each broker in a cluster, since most
  # requests must be directed specifically to the broker that is currently leader for
  # the set of topic partitions you want to produce to or consume from.
  #
  # ## Instrumentation
  #
  # Connections emit a `request.connection.kafka` notification on each request. The following
  # keys will be found in the payload:
  #
  # * `:api` — the name of the API being invoked.
  # * `:request_size` — the number of bytes in the request.
  # * `:response_size` — the number of bytes in the response.
  #
  # The notification also includes the duration of the request.
  #
  class Connection
    SOCKET_TIMEOUT = 10
    CONNECT_TIMEOUT = 10

    # Opens a connection to a Kafka broker.
    #
    # @param host [String] the hostname of the broker.
    # @param port [Integer] the port of the broker.
    # @param client_id [String] the client id is a user-specified string sent in each
    #   request to help trace calls and should logically identify the application
    #   making the request.
    # @param logger [Logger] the logger used to log trace messages.
    # @param connect_timeout [Integer] the socket timeout for connecting to the broker.
    #   Default is 10 seconds.
    # @param socket_timeout [Integer] the socket timeout for reading and writing to the
    #   broker. Default is 10 seconds.
    #
    # @return [Connection] a new connection.
    def initialize(host:, port:, client_id:, logger:, instrumenter:, connect_timeout: nil, socket_timeout: nil,
                   ssl_context: nil, sasl_gssapi_principal: nil, sasl_gssapi_keytab: nil)
      @host, @port, @client_id = host, port, client_id
      @logger = logger
      @instrumenter = instrumenter

      @connect_timeout = connect_timeout || CONNECT_TIMEOUT
      @socket_timeout = socket_timeout || SOCKET_TIMEOUT
      @ssl_context = ssl_context
      @sasl_gssapi_principal = sasl_gssapi_principal
      @sasl_gssapi_keytab = sasl_gssapi_keytab
    end

    def to_s
      "#{@host}:#{@port}"
    end

    def open?
      !@socket.nil? && !@socket.closed?
    end

    def close
      @logger.debug "Closing socket to #{to_s}"

      @socket.close if @socket

      @socket = nil
    end

    # Sends a request over the connection.
    #
    # @param request [#encode, #response_class] the request that should be
    #   encoded and written.
    #
    # @return [Object] the response.
    def send_request(request)
      # Default notification payload.
      notification = {
        broker_host: @host,
        api: Protocol.api_name(request.api_key),
        request_size: 0,
        response_size: 0,
      }

      @instrumenter.instrument("request.connection", notification) do
        open unless open?

        @correlation_id += 1

        @operation.write_request(request, notification, @correlation_id)

        response_class = request.response_class
        @operation.wait_for_response(response_class, notification, @correlation_id) unless response_class.nil?
      end
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError => e
      close

      raise ConnectionError, "Connection error: #{e}"
    end

    private

    def open
      @logger.debug "Opening connection to #{@host}:#{@port} with client id #{@client_id}..."

      if @ssl_context
        @socket = SSLSocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout, ssl_context: @ssl_context)
      end
      if @sasl_gssapi_principal && !@sasl_gssapi_principal.empty?
        @socket = SaslSocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout,
                                            client_id: @client_id, logger: @logger, sasl_gssapi_principal: @principal, sasl_gssapi_keytab: @keytab)
      end
      @socket ||= SocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout)

      encoder = Kafka::Protocol::Encoder.new(@socket)
      decoder = Kafka::Protocol::Decoder.new(@socket)

      @operation = Kafka::ConnectionOperation.new(logger: @logger, client_id: @client_id, encoder: encoder, decoder: decoder)

      # Correlation id is initialized to zero and bumped for each request.
      @correlation_id = 0
    rescue Errno::ETIMEDOUT => e
      @logger.error "Timed out while trying to connect to #{self}: #{e}"
      raise ConnectionError, e
    rescue SocketError, Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
      @logger.error "Failed to connect to #{self}: #{e}"
      raise ConnectionError, e
    end
  end
end
