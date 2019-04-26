# frozen_string_literal: true

module Kafka
  class ZstdCodec
    def codec_id
      4
    end

    def produce_api_min_version
      7
    end

    def load
      require "zstd-ruby"
    rescue LoadError
      raise LoadError, "using zstd compression requires adding a dependency on the `zstd-ruby` gem to your Gemfile."
    end

    def compress(data)
      Zstd.compress(data)
    end

    def decompress(data)
      Zstd.decompress(data)
    end
  end
end
