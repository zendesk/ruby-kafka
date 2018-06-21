# frozen_string_literal: true

module Kafka
  module Protocol
    class FindCoordinatorResponse
      attr_reader :error_code, :error_message

      attr_reader :coordinator_id, :coordinator_host, :coordinator_port

      def initialize(error_code:, error_message:, coordinator_id:, coordinator_host:, coordinator_port:)
        @error_code = error_code
        @coordinator_id = coordinator_id
        @coordinator_host = coordinator_host
        @coordinator_port = coordinator_port
      end

      def self.decode(decoder)
        _throttle_time_ms = decoder.int32
        new(
          error_code: decoder.int16,
          error_message: decoder.string,
          coordinator_id: decoder.int32,
          coordinator_host: decoder.string,
          coordinator_port: decoder.int32,
        )
      end
    end
  end
end
