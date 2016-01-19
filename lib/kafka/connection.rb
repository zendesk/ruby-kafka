require "socket"
require "kafka/protocol/request_message"
require "kafka/protocol/encoder"
require "kafka/protocol/decoder"

module Kafka
  class Connection
    def initialize(host:, port:, client_id:, logger:)
      @host = host
      @port = port
      @client_id = client_id
      @logger = logger
      @socket = nil
      @correlation_id = 0
      @socket_timeout = 1000
    end

    def open
      @logger.info "Opening connection to #{@host}:#{@port} with client id #{@client_id}..."

      @socket = TCPSocket.new(@host, @port)
      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)
    rescue SocketError => e
      @logger.error "Failed to connect to #{@host}:#{@port}: #{e}"

      raise ConnectionError, e
    end

    def write_request(api_key, request)
      @correlation_id += 1

      message = Kafka::Protocol::RequestMessage.new(
        api_key: api_key,
        api_version: 0,
        correlation_id: @correlation_id,
        client_id: @client_id,
        request: request,
      )

      buffer = StringIO.new
      message_encoder = Kafka::Protocol::Encoder.new(buffer)
      message.encode(message_encoder)

      @logger.info "Sending request #{@correlation_id} (#{request.class})..."

      @encoder.write_bytes(buffer.string)
    end

    def read_response(response)
      @logger.info "Reading response #{response.class}"

      bytes = @decoder.bytes

      buffer = StringIO.new(bytes)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32

      @logger.info "Correlation id #{correlation_id}"

      response.decode(response_decoder)
    end
  end
end
