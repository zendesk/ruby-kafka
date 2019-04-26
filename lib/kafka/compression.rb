# frozen_string_literal: true

require "kafka/snappy_codec"
require "kafka/gzip_codec"
require "kafka/lz4_codec"
require "kafka/zstd_codec"

module Kafka
  module Compression
    CODECS_BY_NAME = {
      :gzip => GzipCodec.new,
      :snappy => SnappyCodec.new,
      :lz4 => LZ4Codec.new,
      :zstd => ZstdCodec.new,
    }.freeze

    CODECS_BY_ID = CODECS_BY_NAME.each_with_object({}) do |(_, codec), hash|
      hash[codec.codec_id] = codec
    end.freeze

    def self.codecs
      CODECS_BY_NAME.keys
    end

    def self.find_codec(name)
      codec = CODECS_BY_NAME.fetch(name) do
        raise "Unknown compression codec #{name}"
      end

      codec.load

      codec
    end

    def self.find_codec_by_id(codec_id)
      codec = CODECS_BY_ID.fetch(codec_id) do
        raise "Unknown codec id #{codec_id}"
      end

      codec.load

      codec
    end
  end
end
