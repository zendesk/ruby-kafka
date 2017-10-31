module Kafka
  module Protocol

    class ApiVersionsRequest
      def api_key
        18
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
