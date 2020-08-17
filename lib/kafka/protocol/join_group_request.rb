# frozen_string_literal: true

require "kafka/protocol/consumer_group_protocol"

module Kafka
  module Protocol
    class JoinGroupRequest
      PROTOCOL_TYPE = "consumer"

      def initialize(group_id:, session_timeout:, rebalance_timeout:, member_id:, topics: [], protocol_name:, user_data: nil)
        @group_id = group_id
        @session_timeout = session_timeout * 1000 # Kafka wants ms.
        @rebalance_timeout = rebalance_timeout * 1000 # Kafka wants ms.
        @member_id = member_id || ""
        @protocol_type = PROTOCOL_TYPE
        @group_protocols = {
          protocol_name => ConsumerGroupProtocol.new(topics: topics, user_data: user_data),
        }
      end

      def api_key
        JOIN_GROUP_API
      end

      def api_version
        1
      end

      def response_class
        JoinGroupResponse
      end

      def encode(encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@session_timeout)
        encoder.write_int32(@rebalance_timeout)
        encoder.write_string(@member_id)
        encoder.write_string(@protocol_type)

        encoder.write_array(@group_protocols) do |name, metadata|
          encoder.write_string(name)
          encoder.write_bytes(Encoder.encode_with(metadata))
        end
      end
    end
  end
end
