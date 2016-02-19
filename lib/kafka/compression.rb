require "kafka/snappy_codec"

module Kafka
  module Compression
    def self.find_codec(name)
      case name
      when :snappy then SnappyCodec.new
      else nil
      end
    end

    def self.find_codec_by_id(codec_id)
      case codec_id
      when 2 then SnappyCodec.new
      else raise "Unknown codec id #{codec_id}"
      end
    end
  end
end
