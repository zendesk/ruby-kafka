# frozen_string_literal: true

module Kafka
  module Protocol
    class FindCoordinatorRequest
      def initialize(coordinator_key:, coordinator_type:)
        @coordinator_key  = coordinator_key
        @coordinator_type = coordinator_type
      end

      def api_key
        FIND_COORDINATOR_API
      end

      def api_version
        1
      end

      def encode(encoder)
        encoder.write_string(@coordinator_key)
        encoder.write_int8(@coordinator_type)
      end

      def response_class
        FindCoordinatorResponse
      end
    end
  end
end
