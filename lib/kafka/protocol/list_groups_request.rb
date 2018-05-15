# frozen_string_literal: true

module Kafka
  module Protocol
    class ListGroupsRequest
      def api_key
        LIST_GROUPS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::ListGroupsResponse
      end

      def encode(encoder)
        # noop
      end
    end
  end
end
