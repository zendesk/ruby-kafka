module Kafka
  module Protocol
    class GroupCoordinatorRequest
      def initialize(group_id:)
        @group_id = group_id
      end

      def api_key
        ApiKey::GROUP_COORDINATOR
      end

      def encode(encoder)
        encoder.write_string(@group_id)
      end

      def response_class
        GroupCoordinatorResponse
      end
    end
  end
end
