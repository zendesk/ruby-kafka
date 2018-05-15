# frozen_string_literal: true

module Kafka
  module Protocol
    class HeartbeatRequest
      def initialize(group_id:, generation_id:, member_id:)
        @group_id = group_id
        @generation_id = generation_id
        @member_id = member_id
      end

      def api_key
        HEARTBEAT_API
      end

      def response_class
        HeartbeatResponse
      end

      def encode(encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@generation_id)
        encoder.write_string(@member_id)
      end
    end
  end
end
