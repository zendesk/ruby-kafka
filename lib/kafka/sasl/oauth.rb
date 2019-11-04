# frozen_string_literal: true

module Kafka
  module Sasl
    class OAuth
      OAUTH_IDENT = "OAUTHBEARER"

      # token_provider: THE FOLLOWING INTERFACE MUST BE FULFILLED:
      #
      # [REQUIRED] TokenProvider#token      - Returns an ID/Access Token to be sent to the Kafka client.
      #   The implementation should ensure token reuse so that multiple calls at connect time do not
      #   create multiple tokens. The implementation should also periodically refresh the token in
      #   order to guarantee that each call returns an unexpired token. A timeout error should
      #   be returned after a short period of inactivity so that the broker can log debugging
      #   info and retry.
      #
      # [OPTIONAL] TokenProvider#extensions - Returns a map of key-value pairs that can be sent with the
      #   SASL/OAUTHBEARER initial client response. If not provided, the values are ignored. This feature
      #   is only available in Kafka >= 2.1.0.
      #
      def initialize(logger:, token_provider:)
        @logger = TaggedLogger.new(logger)
        @token_provider = token_provider
      end

      def ident
        OAUTH_IDENT
      end

      def configured?
        @token_provider
      end

      def authenticate!(host, encoder, decoder)
        # Send SASLOauthBearerClientResponse with token
        @logger.debug "Authenticating to #{host} with SASL #{OAUTH_IDENT}"

        encoder.write_bytes(initial_client_response)

        begin
          # receive SASL OAuthBearer Server Response
          msg = decoder.bytes
          raise Kafka::Error, "SASL #{OAUTH_IDENT} authentication failed: unknown error" unless msg
        rescue Errno::ETIMEDOUT, EOFError => e
          raise Kafka::Error, "SASL #{OAUTH_IDENT} authentication failed: #{e.message}"
        end

        @logger.debug "SASL #{OAUTH_IDENT} authentication successful."
      end

      private

      def initial_client_response
        raise Kafka::TokenMethodNotImplementedError, "Token provider doesn't define 'token'" unless @token_provider.respond_to? :token
        "n,,\x01auth=Bearer #{@token_provider.token}#{token_extensions}\x01\x01"
      end

      def token_extensions
        return nil unless @token_provider.respond_to? :extensions
        "\x01#{@token_provider.extensions.map {|e| e.join("=")}.join("\x01")}"
      end
    end
  end
end
