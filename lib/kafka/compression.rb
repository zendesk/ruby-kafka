require "kafka/snappy_codec"
require "kafka/gzip_codec"

module Kafka
  module Compression
    def self.find_codec(name)
      case name
      when nil then nil
      when :snappy then SnappyCodec.new
      when :gzip then GzipCodec.new
      else raise "Unknown compression codec #{name}"
      end
    end

    def self.find_codec_by_id(codec_id)
      case codec_id
      when 1 then GzipCodec.new
      when 2 then SnappyCodec.new
      else raise "Unknown codec id #{codec_id}"
      end
    end
  end
end
