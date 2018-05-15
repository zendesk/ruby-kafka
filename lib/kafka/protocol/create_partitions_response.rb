# frozen_string_literal: true

module Kafka
  module Protocol

    class CreatePartitionsResponse
      attr_reader :errors

      def initialize(throttle_time_ms:, errors:)
        @throttle_time_ms = throttle_time_ms
        @errors = errors
      end

      def self.decode(decoder)
        throttle_time_ms = decoder.int32
        errors = decoder.array do
          topic = decoder.string
          error_code = decoder.int16
          error_message = decoder.string
          [topic, error_code, error_message]
        end

        new(throttle_time_ms: throttle_time_ms, errors: errors)
      end
    end

  end
end
