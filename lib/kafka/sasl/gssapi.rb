# frozen_string_literal: true

module Kafka
  module Sasl
    class Gssapi
      GSSAPI_IDENT = "GSSAPI"
      GSSAPI_CONFIDENTIALITY = false

      def initialize(logger:, principal:, keytab:)
        @logger = TaggedLogger.new(logger)
        @principal = principal
        @keytab = keytab
      end

      def configured?
        @principal && !@principal.empty?
      end

      def ident
        GSSAPI_IDENT
      end

      def authenticate!(host, encoder, decoder)
        load_gssapi
        initialize_gssapi_context(host)

        @encoder = encoder
        @decoder = decoder

        # send gssapi token and receive token to verify
        token_to_verify = send_and_receive_sasl_token

        # verify incoming token
        unless @gssapi_ctx.init_context(token_to_verify)
          raise Kafka::Error, "GSSAPI context verification failed."
        end

        # we can continue, so send OK
        @encoder.write([0, 2].pack('l>c'))

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

      def load_gssapi
        begin
          require "gssapi"
        rescue LoadError
          @logger.error "In order to use GSSAPI authentication you need to install the `gssapi` gem."
          raise
        end
      end

      def initialize_gssapi_context(host)
        @logger.debug "GSSAPI: Initializing context with #{host}, principal #{@principal}"

        @gssapi_ctx = GSSAPI::Simple.new(host, @principal, @keytab)
        @gssapi_token = @gssapi_ctx.init_context(nil)
      end
    end
  end
end
