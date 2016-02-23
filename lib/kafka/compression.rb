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

    def self.compress(codec, data)
      compressed_data = codec.compress(data)

      wrapper_message = Protocol::Message.new(
        value: compressed_data,
        attributes: codec.codec_id,
      )

      message_set = Protocol::MessageSet.new(messages: [wrapper_message])

      Instrumentation.instrument("compress.producer.kafka", {
        codec_id: codec.codec_id,
        original_bytesize: data.bytesize,
        compressed_bytesize: compressed_data.bytesize,
      })

      message_set
    end
  end
end
