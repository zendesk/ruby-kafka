# frozen_string_literal: true

module Kafka
  module Protocol

    class DescribeConfigsRequest
      def initialize(resources:)
        @resources = resources
      end

      def api_key
        DESCRIBE_CONFIGS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::DescribeConfigsResponse
      end

      def encode(encoder)
        encoder.write_array(@resources) do |type, name, configs|
          encoder.write_int8(type)
          encoder.write_string(name)
          encoder.write_array(configs) do |config|
            encoder.write_string(config)
          end
        end
      end
    end

  end
end
