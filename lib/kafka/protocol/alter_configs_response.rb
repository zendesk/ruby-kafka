# frozen_string_literal: true

module Kafka
  module Protocol
    class AlterConfigsResponse
      class ResourceDescription
        attr_reader :name, :type, :error_code, :error_message

        def initialize(name:, type:, error_code:, error_message:)
          @name = name
          @type = type
          @error_code = error_code
          @error_message = error_message
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

          ResourceDescription.new(
            type: RESOURCE_TYPES[resource_type],
            name: resource_name,
            error_code: error_code,
            error_message: error_message
          )
        end

        new(throttle_time_ms: throttle_time_ms, resources: resources)
      end
    end

  end
end
