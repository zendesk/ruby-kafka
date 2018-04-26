# frozen_string_literal: true

module Kafka
  module Protocol

    class ApiVersionsResponse
      class ApiInfo
        attr_reader :api_key, :min_version, :max_version

        def initialize(api_key:, min_version:, max_version:)
          @api_key, @min_version, @max_version = api_key, min_version, max_version
        end

        def api_name
          Protocol.api_name(api_key)
        end

        def version_supported?(version)
          (min_version..max_version).include?(version)
        end

        def to_s
          "#{api_name}=#{min_version}..#{max_version}"
        end

        def inspect
          "#<Kafka api version #{to_s}>"
        end
      end

      attr_reader :error_code, :apis

      def initialize(error_code:, apis:)
        @error_code = error_code
        @apis = apis
      end

      def self.decode(decoder)
        error_code = decoder.int16

        apis = decoder.array do
          ApiInfo.new(
            api_key: decoder.int16,
            min_version: decoder.int16,
            max_version: decoder.int16,
          )
        end

        new(error_code: error_code, apis: apis)
      end
    end
  end
end
