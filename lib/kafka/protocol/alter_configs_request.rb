# frozen_string_literal: true

module Kafka
  module Protocol

    class AlterConfigsRequest
      def initialize(resources:)
        @resources = resources
      end

      def api_key
        ALTER_CONFIGS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::AlterConfigsResponse
      end

      def encode(encoder)
        encoder.write_array(@resources) do |type, name, configs|
          encoder.write_int8(type)
          encoder.write_string(name)

          configs = configs.to_a
          encoder.write_array(configs) do |config_name, config_value|
            # Config value is nullable. In other cases, we must write the
            # stringified value.
            config_value = config_value.to_s unless config_value.nil?

            encoder.write_string(config_name)
            encoder.write_string(config_value)
          end
        end
        # validate_only. We'll skip this feature.
        encoder.write_boolean(false)
      end
    end

  end
end
