require 'gssapi'

module Kafka
  class SaslGssapiAuthenticator
    GSSAPI_IDENT = "GSSAPI"
    GSSAPI_CONFIDENTIALITY = false

    def initialize(connection:, logger:, sasl_gssapi_principal:, sasl_gssapi_keytab:)
      @connection = connection
      @logger = logger
      @principal = sasl_gssapi_principal
      @keytab = sasl_gssapi_keytab

      initialize_gssapi_context
    end

    def authenticate!
      proceed_sasl_gssapi_negotiation
    end

    private

    def proceed_sasl_gssapi_negotiation
      @encoder = @connection.encoder
      @decoder = @connection.decoder

      response = @connection.send_request(Kafka::Protocol::SaslHandshakeRequest.new(GSSAPI_IDENT))
      unless response.error_code == 0 && response.enabled_mechanisms.include?(GSSAPI_IDENT)
        raise Kafka::Error, "#{GSSAPI_IDENT} is not supported."
      end

      # send gssapi token and receive token to verify
      token_to_verify = send_and_receive_sasl_token

      # verify incoming token
      unless @gssapi_ctx.init_context(token_to_verify)
        raise Kafka::Error, "GSSAPI context verification failed."
      end

      # we can continue, so send OK
      @encoder.write([0,2].pack('l>c'))

      # read wrapped message and return it back with principal
      handshake_messages
    end

    def handshake_messages
      msg = @decoder.bytes
      raise Kafka::Error, "GSSAPI negotiation failed." unless msg
      # unwrap with integrity only
      msg_unwrapped = @gssapi_ctx.unwrap_message(msg, GSSAPI_CONFIDENTIALITY)
      msg_wrapped = @gssapi_ctx.wrap_message(msg_unwrapped + @principal, GSSAPI_CONFIDENTIALITY)
      @encoder.write_bytes(msg_wrapped)
    end

    def send_and_receive_sasl_token
      @encoder.write_bytes(@gssapi_token)
      @decoder.bytes
    end

    def initialize_gssapi_context
      @logger.debug "GSSAPI: Initializing context with #{@host}, principal #{@principal}"

      @gssapi_ctx = GSSAPI::Simple.new(@host, @principal, @keytab)
      @gssapi_token = @gssapi_ctx.init_context(nil)
    end
  end
end
