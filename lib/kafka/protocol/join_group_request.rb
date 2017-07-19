require "kafka/protocol/consumer_group_protocol"

module Kafka
  module Protocol
    class JoinGroupRequest
      PROTOCOL_TYPE = "consumer"

      def initialize(group_id:, session_timeout:, member_id:, topics: [])
        @group_id = group_id
        @session_timeout = session_timeout * 1000 # Kafka wants ms.
        @member_id = member_id || ""
        @protocol_type = PROTOCOL_TYPE
        @group_protocols = {
          "standard" => ConsumerGroupProtocol.new(topics: ["test-messages"]),
        }
      end

      def api_key
        JOIN_GROUP_API
      end

      def response_class
        JoinGroupResponse
      end

      def encode(encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@session_timeout)
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
