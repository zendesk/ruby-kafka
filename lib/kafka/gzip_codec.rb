module Kafka
  class GzipCodec
    def initialize
      require "zlib"
    end

    def codec_id
      1
    end

    def compress(data)
      buffer = StringIO.new
      buffer.set_encoding(Encoding::BINARY)

      writer = Zlib::GzipWriter.new(buffer, Zlib::DEFAULT_COMPRESSION, Zlib::DEFAULT_STRATEGY)
      writer.write(data)
      writer.close

      buffer.string
    end

    def decompress(data)
      buffer = StringIO.new(data)
      reader = Zlib::GzipReader.new(buffer)
      reader.read
    end
  end
end
