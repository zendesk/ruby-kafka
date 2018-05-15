# frozen_string_literal: true

module Kafka
  module Protocol

    class ApiVersionsRequest
      def api_key
        API_VERSIONS_API
      end

      def encode(encoder)
        # Nothing to do.
      end

      def response_class
        Protocol::ApiVersionsResponse
      end
    end

  end
end
