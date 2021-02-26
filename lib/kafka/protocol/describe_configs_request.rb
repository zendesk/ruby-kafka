# frozen_string_literal: true

module Kafka
  module Protocol

    class DescribeConfigsRequest
      def initialize(resources:, include_synonyms: false)
        @resources = resources
        @include_synonyms = include_synonyms
      end

      def api_key
        DESCRIBE_CONFIGS_API
      end

      def api_version
        1
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
        encoder.write_boolean(@include_synonyms)
      end
    end

  end
end
