require 'socket'
require 'gssapi'

module Kafka
  class SaslSocketWithTimeout < SocketWithTimeout
    GSSAPI_IDENT = "GSSAPI"
    GSSAPI_CONFIDENTIALITY = false

    def initialize(host, port, connect_timeout: nil, timeout: nil, client_id:, logger:, principal:, keytab: nil)
      @host = host
      @port = port
      @logger = logger
      @client_id = client_id
      @principal = principal
      @keytab = keytab

      initialize_gssapi_context

      addr = Socket.getaddrinfo(@host, nil)
      sockaddr = Socket.pack_sockaddr_in(@port, addr[0][3])

      @connect_timeout = connect_timeout
      @timeout = timeout

      @socket = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        @socket.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        # IO.select will block until the socket is writable or the timeout
        # is exceeded, whichever comes first.
        unless IO.select(nil, [@socket], nil, connect_timeout)
          # IO.select returns nil when the socket is not ready before timeout
          # seconds have elapsed
          @socket.close
          raise Errno::ETIMEDOUT
        end

        begin
          # Verify there is now a good connection.
          @socket.connect_nonblock(sockaddr)
        rescue Errno::EISCONN
          # The socket is connected, we're good!
          @correlation_id = 0
          @logger.debug "GSSAPI: Started GSSAPI negotiation."
          proceed_sasl_gssapi_negotiation
        end
      end
    end

    private

    def send_request(request)
      # Default notification payload.
      notification = {
        broker_host: @host,
        api: Protocol.api_name(request.api_key),
        request_size: 0,
        response_size: 0,
      }

      @correlation_id += 1

      @operation.write_request(request, notification, @correlation_id)

      response_class = request.response_class
      @operation.wait_for_response(response_class, notification, @correlation_id) unless response_class.nil?
    rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError => e
      close

      raise ConnectionError, "Connection error: #{e}"
    end

    def proceed_sasl_gssapi_negotiation
      @encoder = Kafka::Protocol::Encoder.new(@socket)
      @decoder = Kafka::Protocol::Decoder.new(@socket)

      @operation = Kafka::ConnectionOperation.new(logger: @logger, client_id: @client_id, encoder: @encoder, decoder: @decoder)
      response = send_request(Kafka::Protocol::SaslHandshakeRequest.new(GSSAPI_IDENT))
      unless response.error_code == 0 && response.enabled_mechanisms.include?(GSSAPI_IDENT)
        raise Kafka::Error, "#{GSSAPI_IDENT} is not supported."
      end

      # send gssapi token and receive token to verify
      token_to_verify = send_sasl_token

      # verify incoming token
      unless @gssapi_ctx.init_context(token_to_verify)
        raise Kafka::Error, "GSSAPI context verification failed."
      end

      # we can continue, so send OK
      @encoder.write([0,2].pack('l>c'))

      # read wrapped message and return it back with principal
      handshake_messages

      # no error, return @socket
      @socket
    end

    def handshake_messages
      msg = @decoder.read(@decoder.int32)
      raise Kafka::Error, "GSSAPI negotiation failed." unless msg
      # unwrap with integrity only
      msg_unwrapped = @gssapi_ctx.unwrap_message(msg, GSSAPI_CONFIDENTIALITY)
      msg_wrapped = @gssapi_ctx.wrap_message(msg_unwrapped + @principal, GSSAPI_CONFIDENTIALITY)
      @encoder.write(msg_with_size(msg_wrapped))
    end

    def msg_with_size(msg)
      [msg.size].pack('l>') + msg
    end

    def send_sasl_token
      @encoder.write(msg_with_size(@gssapi_token))
      @decoder.read(@decoder.int32)
    end

    def initialize_gssapi_context
      @logger.debug "GSSAPI: Initializing context with #{@host}, principal #{@principal}"

      @gssapi_ctx = GSSAPI::Simple.new(@host, @principal, @keytab)
      @gssapi_token = @gssapi_ctx.init_context(nil)
    end
  end
end
