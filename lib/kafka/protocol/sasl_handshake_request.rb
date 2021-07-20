# frozen_string_literal: true

module Kafka
  module Protocol

    # SaslHandshake Request (Version: 0) => mechanism
    #  mechanism => string

    class SaslHandshakeRequest

      SUPPORTED_MECHANISMS = %w(AWS_MSK_IAM GSSAPI PLAIN SCRAM-SHA-256 SCRAM-SHA-512 OAUTHBEARER)

      def initialize(mechanism)
        unless SUPPORTED_MECHANISMS.include?(mechanism)
          raise Kafka::Error, "Unsupported SASL mechanism #{mechanism}. Supported are #{SUPPORTED_MECHANISMS.join(', ')}"
        end
        @mechanism = mechanism
      end

      def api_key
        SASL_HANDSHAKE_API
      end

      def response_class
        SaslHandshakeResponse
      end

      def encode(encoder)
        encoder.write_string(@mechanism)
      end
    end
  end
end
