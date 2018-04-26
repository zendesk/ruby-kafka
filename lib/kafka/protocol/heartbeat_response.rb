# frozen_string_literal: true

module Kafka
  module Protocol
    class HeartbeatResponse
      attr_reader :error_code

      def initialize(error_code:)
        @error_code = error_code
      end

      def self.decode(decoder)
        new(error_code: decoder.int16)
      end
    end
  end
end
