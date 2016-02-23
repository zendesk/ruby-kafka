require "kafka/snappy_codec"
require "kafka/gzip_codec"

module Kafka
  module Compression
    CODECS = [SnappyCodec.new, GzipCodec.new]

    CODECS_BY_NAME = CODECS.map {|codec| [codec.name, codec] }.to_h
    CODECS_BY_ID = CODECS.map {|codec| [codec.codec_id, codec] }.to_h

    def self.find_codec(name)
      return nil if name.nil?

      CODECS_BY_NAME.fetch(name) {
        raise "Unknown compression codec #{name}"
      }
    end

    def self.find_codec_by_id(codec_id)
      return nil if codec_id.nil?

      CODECS_BY_ID.fetch(codec_id) {
        raise "Unknown codec id #{codec_id}"
      }
    end

    def self.compress(codec, data)
      compressed_data = codec.compress(data)

      wrapper_message = Protocol::Message.new(
        value: compressed_data,
        attributes: codec.codec_id,
      )

      message_set = Protocol::MessageSet.new(messages: [wrapper_message])

      Instrumentation.instrument("compress.producer.kafka", {
        codec_name: codec.name,
        codec_id: codec.codec_id,
        original_bytesize: data.bytesize,
        compressed_bytesize: compressed_data.bytesize,
      })

      message_set
    end
  end
end
