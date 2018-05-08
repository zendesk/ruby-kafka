# frozen_string_literal: true

module Kafka
  module Protocol
    class DescribeGroupsRequest
      def initialize(group_ids:)
        @group_ids = group_ids
      end

      def api_key
        DESCRIBE_GROUPS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::DescribeGroupsResponse
      end

      def encode(encoder)
        encoder.write_array(@group_ids) { |group_id| encoder.write_string(group_id) }
      end
    end
  end
end
