module Kafka
  module Protocol
    class JoinGroupRequest
      PROTOCOL_TYPE = "consumer"

      def initialize(group_id:, session_timeout:, member_id:, topics: [], group_protocols: {})
        @group_id = group_id
        @session_timeout = session_timeout * 1000 # Kafka wants ms.
        @member_id = member_id || ""
        @protocol_type = PROTOCOL_TYPE
        @group_protocols = group_protocols
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

        encoder.write_array(@group_protocols) do |group_protocol|
          encoder.write_string(group_protocol.name)
          encoder.write_bytes(group_protocol.metadata)
        end
      end
    end
  end
end
