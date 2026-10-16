# frozen_string_literal: true

module Kafka
  module Protocol

    class DeleteTopicsResponse
      attr_reader :errors

      def initialize(errors:)
        @errors = errors
      end

      def self.decode(decoder)
        decoder.int32 # throttle_time_ms
        errors = decoder.array do
          topic = decoder.string
          error_code = decoder.int16

          [topic, error_code]
        end

        new(errors: errors)
      end
    end

  end
end
