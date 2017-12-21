require "kafka/snappy_codec"
require "kafka/gzip_codec"
require "kafka/lz4_codec"

module Kafka
  module Compression
    CODEC_NAMES = {
      1 => :gzip,
      2 => :snappy,
      3 => :lz4,
    }.freeze

    CODECS = {
      :gzip => GzipCodec.new,
      :snappy => SnappyCodec.new,
      :lz4 => LZ4Codec.new,
    }.freeze

    def self.codecs
      CODECS.keys
    end

    def self.find_codec(name)
      codec = CODECS.fetch(name) do
        raise "Unknown compression codec #{name}"
      end

      codec.load

      codec
    end

    def self.find_codec_by_id(codec_id)
      codec_name = CODEC_NAMES.fetch(codec_id) do
        raise "Unknown codec id #{codec_id}"
      end

      find_codec(codec_name)
    end
  end
end
