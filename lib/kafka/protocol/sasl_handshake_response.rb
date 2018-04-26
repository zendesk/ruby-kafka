# frozen_string_literal: true

module Kafka
  module Protocol

    # SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms]
    #  error_code => int16
    #  enabled_mechanisms => array of strings

    class SaslHandshakeResponse
      attr_reader :error_code

      attr_reader :enabled_mechanisms

      def initialize(error_code:, enabled_mechanisms:)
        @error_code = error_code
        @enabled_mechanisms = enabled_mechanisms
      end

      def self.decode(decoder)
        new(
          error_code: decoder.int16,
          enabled_mechanisms: decoder.array { decoder.string }
        )
      end
    end
  end
end
