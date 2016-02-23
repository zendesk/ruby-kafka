module Kafka
  class SnappyCodec
    def initialize
      require "snappy"
    end

    def name
      :snappy
    end

    def codec_id
      2
    end

    def compress(data)
      Snappy.deflate(data)
    end

    def decompress(data)
      buffer = StringIO.new(data)
      Snappy::Reader.new(buffer).read
    end
  end
end
