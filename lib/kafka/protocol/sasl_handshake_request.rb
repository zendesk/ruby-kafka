module Kafka
  module Protocol

    # SaslHandshake Request (Version: 0) => mechanism
    #  mechanism => string

    class SaslHandshakeRequest

      def initialize(mechanism)
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
