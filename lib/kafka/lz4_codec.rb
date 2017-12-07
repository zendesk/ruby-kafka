module Kafka
  class LZ4Codec
    def initialize
      require "lz4-ruby"
    rescue LoadError
      raise LoadError, "using lz4 compression requires adding a dependency on the `lz4-ruby` gem to your Gemfile."
    end

    def codec_id
      3
    end

    def compress(data)
      LZ4::compress(data)
    end

    def decompress(data)
      buffer = StringIO.new(data)
      LZ4::uncompress(buffer)
    end
  end
end
