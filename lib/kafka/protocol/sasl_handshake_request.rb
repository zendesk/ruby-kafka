module Kafka
  module Protocol

    # SaslHandshake Request (Version: 0) => mechanism
    #  mechanism => string

    class SaslHandshakeRequest

      SUPPORTED_MECHANISMS = %w(GSSAPI)

      def initialize(mechanism)
        unless SUPPORTED_MECHANISMS.include?(mechanism)
          raise Kafka::Error, "Unsupported SASL mechanism #{mechanism}. Supported are #{SUPPORTED_MECHANISMS.join(', ')}"
        end
        @mechanism = mechanism
      end

      def api_key
        17
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
