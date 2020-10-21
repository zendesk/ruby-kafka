# frozen_string_literal: true

module Kafka
  module Protocol
    class AddOffsetsToTxnResponse

      attr_reader :error_code

      def initialize(error_code:)
        @error_code = error_code
      end

      def self.decode(decoder)
        _throttle_time_ms = decoder.int32
        error_code = decoder.int16
        new(error_code: error_code)
      end

    end
  end
end
