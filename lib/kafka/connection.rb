# frozen_string_literal: true

require "stringio"
require "kafka/socket_with_timeout"
require "kafka/ssl_socket_with_timeout"
require "kafka/protocol/request_message"
require "kafka/protocol/encoder"
require "kafka/protocol/decoder"

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

    # Time after which an idle connection will be reopened.
    IDLE_TIMEOUT = 60 * 5

    attr_reader :encoder
    attr_reader :decoder

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
    def initialize(host:, port:, client_id:, logger:, instrumenter:, connect_timeout: nil, socket_timeout: nil, ssl_context: nil)
      @host, @port, @client_id = host, port, client_id
      @logger = TaggedLogger.new(logger)
      @instrumenter = instrumenter

      @connect_timeout = connect_timeout || CONNECT_TIMEOUT
      @socket_timeout = socket_timeout || SOCKET_TIMEOUT
      @ssl_context = ssl_context

      @socket = nil
      @last_request = nil
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
    end

    # Sends a request over the connection.
    #
    # @param request [#encode, #response_class] the request that should be
    #   encoded and written.
    #
    # @return [Object] the response.
    def send_request(request)
      api_name = Protocol.api_name(request.api_key)

      # Default notification payload.
      notification = {
        broker_host: @host,
        api: api_name,
        request_size: 0,
        response_size: 0,
      }

      raise IdleConnection if idle?

      @logger.push_tags(api_name)
      @instrumenter.instrument("request.connection", notification) do
        open unless open?

        @correlation_id += 1

        @logger.debug "Sending #{api_name} API request #{@correlation_id} to #{to_s}"

        write_request(request, notification)

        response_class = request.response_class
        response = wait_for_response(response_class, notification) unless response_class.nil?

        @last_request = Time.now

        response
      end
    rescue SystemCallError, EOFError, IOError => e
      close

      raise ConnectionError, "Connection error #{e.class}: #{e}"
    ensure
      @logger.pop_tags
    end

    private

    def open
      @logger.debug "Opening connection to #{@host}:#{@port} with client id #{@client_id}..."

      if @ssl_context
        @socket = SSLSocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout, ssl_context: @ssl_context)
      else
        @socket = SocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout)
      end

      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)

      # Correlation id is initialized to zero and bumped for each request.
      @correlation_id = 0

      @last_request = nil
    rescue Errno::ETIMEDOUT => e
      @logger.error "Timed out while trying to connect to #{self}: #{e}"
      raise ConnectionError, e
    rescue SocketError, Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
      @logger.error "Failed to connect to #{self}: #{e}"
      raise ConnectionError, e
    end

    def idle?
      @last_request && @last_request < Time.now - IDLE_TIMEOUT
    end

    # Writes a request over the connection.
    #
    # @param request [#encode] the request that should be encoded and written.
    #
    # @return [nil]
    def write_request(request, notification)
      message = Kafka::Protocol::RequestMessage.new(
        api_key: request.api_key,
        api_version: request.respond_to?(:api_version) ? request.api_version : 0,
        correlation_id: @correlation_id,
        client_id: @client_id,
        request: request,
      )

      data = Kafka::Protocol::Encoder.encode_with(message)
      notification[:request_size] = data.bytesize

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
    def read_response(response_class, notification)
      @logger.debug "Waiting for response #{@correlation_id} from #{to_s}"

      data = @decoder.bytes
      notification[:response_size] = data.bytesize

      buffer = StringIO.new(data)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32
      response = response_class.decode(response_decoder)

      @logger.debug "Received response #{correlation_id} from #{to_s}"

      return correlation_id, response
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while waiting for response #{@correlation_id}"
      raise
    end

    def wait_for_response(response_class, notification)
      loop do
        correlation_id, response = read_response(response_class, notification)

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
