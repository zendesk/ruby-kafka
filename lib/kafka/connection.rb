require "socket"
require "kafka/protocol/request_message"
require "kafka/protocol/encoder"
require "kafka/protocol/decoder"

module Kafka

  # A connection to a single Kafka broker.
  #
  # Usually you'll need a separate connection to each broker in a cluster, since most
  # requests must be directed specifically to the broker that is currently leader for
  # the set of topic partitions you want to produce to or consumer from.
  class Connection
    API_VERSION = 0

    # Opens a connection to a Kafka broker.
    #
    # @param host [String] the hostname of the broker.
    # @param port [Integer] the port of the broker.
    # @param client_id [String] the client id is a user-specified string sent in each
    #   request to help trace calls and should logically identify the application
    #   making the request.
    # @param logger [Logger] the logger used to log trace messages.
    #
    # @return [Connection] a new connection.
    def self.open(host:, port:, client_id:, logger:)
      logger.info "Opening connection to #{host}:#{port} with client id #{client_id}..."

      socket = TCPSocket.new(host, port)
      new(socket: socket, client_id: client_id, logger: logger)
    rescue SocketError => e
      logger.error "Failed to connect to #{host}:#{port}: #{e}"

      raise ConnectionError, e
    end

    def initialize(socket:, client_id:, logger:)
      @client_id = client_id
      @logger = logger
      @socket = socket
      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)
      @correlation_id = 0
    end

    # Sends a request over the connection.
    #
    # @param api_key [Integer] the integer code for the API that is invoked.
    # @param request [#encode] the request that should be encoded and written.
    # @param response_class [#decode] an object that can decode the response.
    #
    # @return [Object] the response that was decoded by `response_class`.
    def request(api_key, request, response_class)
      write_request(api_key, request)

      unless response_class.nil?
        read_response(response_class)
      end
    end

    private

    # Writes a request over the connection.
    #
    # @param api_key [Integer] the integer code for the API that is invoked.
    # @param request [#encode] the request that should be encoded and written.
    #
    # @return [nil]
    def write_request(api_key, request)
      @correlation_id += 1
      @logger.debug "Sending request #{@correlation_id}"

      message = Kafka::Protocol::RequestMessage.new(
        api_key: api_key,
        api_version: API_VERSION,
        correlation_id: @correlation_id,
        client_id: @client_id,
        request: request,
      )

      data = Kafka::Protocol::Encoder.encode_with(message)
      @encoder.write_bytes(data)

      nil
    end

    # Reads a response from the connection.
    #
    # @param response_class [#decode] an object that can decode the response from
    #   a given Decoder.
    #
    # @return [nil]
    def read_response(response_class)
      @logger.debug "Waiting for response #{@correlation_id}"

      bytes = @decoder.bytes

      buffer = StringIO.new(bytes)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32
      response = response_class.decode(response_decoder)

      @logger.debug "Received response #{correlation_id}"

      response
    end
  end
end
