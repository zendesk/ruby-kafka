module Kafka
  module Protocol

    class ApiVersionsResponse
      class ApiVersion
        attr_reader :api_key, :min_version, :max_version

        def initialize(api_key:, min_version:, max_version:)
          @api_key, @min_version, @max_version = api_key, min_version, max_version
        end

        def api_name
          Protocol.api_name(api_key)
        end

        def to_s
          "#{api_name}=#{min_version}..#{max_version}"
        end

        def inspect
          "#<Kafka api version #{to_s}>"
        end
      end

      attr_reader :error_code, :api_versions

      def initialize(error_code:, api_versions:)
        @error_code = error_code
        @api_versions = api_versions
      end

      def self.decode(decoder)
        error_code = decoder.int16

        api_versions = decoder.array do
          ApiVersion.new(
            api_key: decoder.int16,
            min_version: decoder.int16,
            max_version: decoder.int16,
          )
        end

        new(error_code: error_code, api_versions: api_versions)
      end
    end
  end
end
