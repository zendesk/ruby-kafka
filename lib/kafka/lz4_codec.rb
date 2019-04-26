# frozen_string_literal: true

module Kafka
  class LZ4Codec
    def codec_id
      3
    end

    def produce_api_min_version
      0
    end

    def load
      require "extlz4"
    rescue LoadError
      raise LoadError, "using lz4 compression requires adding a dependency on the `extlz4` gem to your Gemfile."
    end

    def compress(data)
      LZ4.encode(data)
    end

    def decompress(data)
      LZ4.decode(data)
    end
  end
end
