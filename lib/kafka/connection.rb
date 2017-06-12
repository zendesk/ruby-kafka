require "stringio"
require "kafka/socket_with_timeout"
require "kafka/ssl_socket_with_timeout"
require "kafka/protocol/request_message"
require "kafka/protocol/null_response"
require "kafka/protocol/encoder"
require "kafka/protocol/decoder"

module Kafka

  # An asynchronous response object allows us to deliver a response at some
  # later point in time.
  #
  # When instantiating an AsyncResponse, you provide a response decoder and
  # a block that will force the caller to wait until a response is available.
  class AsyncResponse
    # Use a custom "nil" value so that nil can be an actual value.
    MISSING = Object.new

    def initialize(decoder, &block)
      @decoder = decoder
      @block = block
      @response = MISSING
    end

    # Block until a response is available.
    def call
      @block.call if @response == MISSING
      @response
    end

    # Deliver the response data.
    #
    # After calling this, `#call` will returned the decoded response.
    def deliver(data)
      @response = @decoder.decode(data)
    end
  end

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
    def initialize(host:, port:, client_id:, logger:, instrumenter:, connect_timeout: nil, socket_timeout: nil, ssl_context: nil)
      @host, @port, @client_id = host, port, client_id
      @logger = logger
      @instrumenter = instrumenter

      @connect_timeout = connect_timeout || CONNECT_TIMEOUT
      @socket_timeout = socket_timeout || SOCKET_TIMEOUT
      @ssl_context = ssl_context
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
      # Immediately block on the asynchronous request.
      send_async_request(request).call
    end

    # Sends a request over the connection.
    #
    # @param request [#encode, #response_class] the request that should be
    #   encoded and written.
    #
    # @return [AsyncResponse] the async response, allowing the caller to choose
    #   when to block.
    def send_async_request(request)
      # Default notification payload.
      notification = {
        broker_host: @host,
        api: Protocol.api_name(request.api_key),
        request_size: 0,
        response_size: 0,
      }

      @instrumenter.start("request.connection", notification)

      open unless open?

      @correlation_id += 1

      write_request(request, notification)

      response_class = request.response_class
      correlation_id = @correlation_id

      if response_class.nil?
        async_response = AsyncResponse.new(Protocol::NullResponse) { nil }

        # Immediately deliver a nil value.
        async_response.deliver(nil)

        @instrumenter.finish("request.connection", notification)

        async_response
      else
        async_response = AsyncResponse.new(response_class) {
          # A caller is trying to read the response, so we have to wait for it
          # before we can return.
          wait_for_response(correlation_id, notification)

          # Once done, we can finish the instrumentation.
          @instrumenter.finish("request.connection", notification)
        }

        # Store the asynchronous response so that data can be delivered to it
        # at a later time.
        @pending_async_responses[correlation_id] = async_response

        async_response
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
      else
        @socket = SocketWithTimeout.new(@host, @port, connect_timeout: @connect_timeout, timeout: @socket_timeout)
      end

      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)

      # Correlation id is initialized to zero and bumped for each request.
      @correlation_id = 0

      # The pipeline of pending response futures must be reset.
      @pending_async_responses = {}
    rescue Errno::ETIMEDOUT => e
      @logger.error "Timed out while trying to connect to #{self}: #{e}"
      raise ConnectionError, e
    rescue SocketError, Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
      @logger.error "Failed to connect to #{self}: #{e}"
      raise ConnectionError, e
    end

    # Writes a request over the connection.
    #
    # @param request [#encode] the request that should be encoded and written.
    #
    # @return [nil]
    def write_request(request, notification)
      @logger.debug "Sending request #{@correlation_id} to #{to_s}"

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
    def read_response(expected_correlation_id, notification)
      @logger.debug "Waiting for response #{expected_correlation_id} from #{to_s}"

      data = @decoder.bytes
      notification[:response_size] = data.bytesize

      buffer = StringIO.new(data)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32

      @logger.debug "Received response #{correlation_id} from #{to_s}"

      return correlation_id, response_decoder
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while waiting for response #{expected_correlation_id}"
      raise
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError => e
      close

      raise ConnectionError, "Connection error: #{e}"
    end

    def wait_for_response(expected_correlation_id, notification)
      loop do
        correlation_id, data = read_response(expected_correlation_id, notification)

        if correlation_id < expected_correlation_id
          # There may have been a previous request that timed out before the client
          # was able to read the response. In that case, the response will still be
          # sitting in the socket waiting to be read. If the response we just read
          # was to a previous request, we deliver it to the pending async response
          # future.
          async_response = @pending_async_responses.delete(correlation_id)
          async_response.deliver(data) if async_response
        elsif correlation_id > expected_correlation_id
          raise Kafka::Error, "Correlation id mismatch: expected #{expected_correlation_id} but got #{correlation_id}"
        else
          # If the request was asynchronous, deliver the response to the pending
          # async response future.
          async_response = @pending_async_responses.delete(correlation_id)
          async_response.deliver(data)

          return async_response.call
        end
      end
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError => e
      notification[:exception] = [e.class.name, e.message]
      notification[:exception_object] = e

      close

      raise ConnectionError, "Connection error: #{e}"
    end
  end
end
