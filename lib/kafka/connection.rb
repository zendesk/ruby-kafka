require "stringio"
require "kafka/socket_with_timeout"
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
    def initialize(host:, port:, client_id:, logger:, connect_timeout: nil, socket_timeout: nil)
      @host, @port, @client_id = host, port, client_id
      @logger = logger

      @connect_timeout = connect_timeout || CONNECT_TIMEOUT
      @socket_timeout = socket_timeout || SOCKET_TIMEOUT
    end

    def to_s
      "#{@host}:#{@port}"
    end

    def open?
      !@socket.nil?
    end

    def close
      @logger.debug "Closing socket to #{to_s}"

      @socket.close if @socket

      @socket = nil
    end

    # Sends a request over the connection.
    #
    # @param request [#encode] the request that should be encoded and written.
    # @param response_class [#decode] an object that can decode the response.
    #
    # @return [Object] the response that was decoded by `response_class`.
    def send_request(request, response_class)
      open unless open?

      @correlation_id += 1

      write_request(request)
      wait_for_response(response_class) unless response_class.nil?
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError => e
      @logger.error "Connection error: #{e}"

      close

      raise ConnectionError, "Connection error: #{e}"
    end

    private

    def open
      @logger.info "Opening connection to #{@host}:#{@port} with client id #{@client_id}..."

      @socket = SocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout)

      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)

      # Correlation id is initialized to zero and bumped for each request.
      @correlation_id = 0
    rescue Errno::ETIMEDOUT => e
      @logger.error "Timed out while trying to connect to #{self}: #{e}"
      raise ConnectionError, e
    rescue SocketError, Errno::ECONNREFUSED => e
      @logger.error "Failed to connect to #{self}: #{e}"
      raise ConnectionError, e
    end

    # Writes a request over the connection.
    #
    # @param api_key [Integer] the integer code for the API that is invoked.
    # @param request [#encode] the request that should be encoded and written.
    #
    # @return [nil]
    def write_request(request)
      @logger.debug "Sending request #{@correlation_id} to #{to_s}"

      message = Kafka::Protocol::RequestMessage.new(
        api_key: request.api_key,
        correlation_id: @correlation_id,
        client_id: @client_id,
        request: request,
      )

      data = Kafka::Protocol::Encoder.encode_with(message)

      @encoder.write_bytes(data)

      nil
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while writing request #{@correlation_id}"
      raise
    end

    # Reads a response from the connection.
    #
    # @param response_class [#decode] an object that can decode the response from
    #   a given Decoder.
    #
    # @return [nil]
    def read_response(response_class)
      @logger.debug "Waiting for response #{@correlation_id} from #{to_s}"

      bytes = @decoder.bytes

      buffer = StringIO.new(bytes)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32
      response = response_class.decode(response_decoder)

      @logger.debug "Received response #{correlation_id} from #{to_s}"

      return correlation_id, response
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while waiting for response #{@correlation_id}"
      raise
    end

    def wait_for_response(response_class)
      loop do
        correlation_id, response = read_response(response_class)

        # There may have been a previous request that timed out before the client
        # was able to read the response. In that case, the response will still be
        # sitting in the socket waiting to be read. If the response we just read
        # was to a previous request, we can safely skip it.
        if correlation_id < @correlation_id
          @logger.error "Received out-of-order response id #{correlation_id}, was expecting #{@correlation_id}"
        elsif correlation_id > @correlation_id
          raise Kafka::Error, "Correlation id mismatch: expected #{@correlation_id} but got #{correlation_id}"
        else
          return response
        end
      end
    end
  end
end
