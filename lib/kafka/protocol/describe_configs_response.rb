# frozen_string_literal: true

module Kafka
  module Protocol
    class DescribeConfigsResponse
      class ResourceDescription
        attr_reader :name, :type, :error_code, :error_message, :configs

        def initialize(name:, type:, error_code:, error_message:, configs:)
          @name = name
          @type = type
          @error_code = error_code
          @error_message = error_message
          @configs = configs
        end
      end

      class ConfigEntry
        attr_reader :name, :value, :read_only, :config_source, :is_sensitive, :synonyms

        def initialize(name:, value:, read_only:, config_source:, is_sensitive:, synonyms:)
          @name = name
          @value = value
          @read_only = read_only
          @is_default = is_default
          @is_sensitive = is_sensitive
          @synonyms = synonyms
        end
      end

      class SynonymEntry
        attr_reader :name, :value, :source

        def initialize(name:, value:, source:)
          @name = name
          @value = value
          @source = source
        end
      end

      attr_reader :resources

      def initialize(throttle_time_ms:, resources:)
        @throttle_time_ms = throttle_time_ms
        @resources = resources
      end

      def self.decode(decoder)
        throttle_time_ms = decoder.int32
        resources = decoder.array do
          error_code = decoder.int16
          error_message = decoder.string

          resource_type = decoder.int8
          if Kafka::Protocol::RESOURCE_TYPES[resource_type].nil?
            raise Kafka::ProtocolError, "Resource type not supported: #{resource_type}"
          end
          resource_name = decoder.string

          configs = decoder.array do
            ConfigEntry.new(
              name: decoder.string,
              value: decoder.string,
              read_only: decoder.boolean,
              config_source: decoder.int8,
              is_sensitive: decoder.boolean,
              synonyms: decoder.array do
                SynonymEntry.new(
                  name: decoder.string,
                  value: decoder.string,
                  source: decoder.int8
                )
              end
            )
          end

          ResourceDescription.new(
            type: RESOURCE_TYPES[resource_type],
            name: resource_name,
            error_code: error_code,
            error_message: error_message,
            configs: configs
          )
        end

        new(throttle_time_ms: throttle_time_ms, resources: resources)
      end
    end
  end
end
