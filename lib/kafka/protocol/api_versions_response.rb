module Kafka
  module Protocol

    class ApiVersionsResponse
      class ApiVersion
        attr_reader :api_key, :min_version, :max_version

        def initialize(api_key:, min_version:, max_version:)
          @api_key, @min_version, @max_version = api_key, min_version, max_version
        end

        def versions
          @versions ||= (min_version..max_version).to_a
        end
      end

      class SupportedVersions
        def initialize(api_versions)
          @api_versions = api_versions
        end

        def find(*args)
          @api_versions.find(*args)
        end

        def for_api(api_key)
          @api_versions.find {|v| v.api_key == api_key }
        end

        def supported_version(api_key, proposed_versions)
          compatible_versions = for_api(api_key).versions & proposed_versions
          compatible_versions.max
        end

        def to_s
          @api_versions.map {|version|
            name = Protocol.api_name(version.api_key)
            "#{name} (min=#{version.min_version}, max=#{version.max_version})"
          }.join("; ")
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

        new(error_code: error_code, api_versions: SupportedVersions.new(api_versions))
      end
    end
  end
end
