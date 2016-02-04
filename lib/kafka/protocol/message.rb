require "stringio"
require "zlib"

module Kafka
  module Protocol

    # ## API Specification
    #
    #     Message => Crc MagicByte Attributes Key Value
    #         Crc => int32
    #         MagicByte => int8
    #         Attributes => int8
    #         Key => bytes
    #         Value => bytes
    #
    class Message
      MAGIC_BYTE = 0

      attr_reader :key, :value, :attributes

      def initialize(key:, value:, attributes: 0)
        @key = key
        @value = value
        @attributes = attributes
      end

      def encode(encoder)
        data = encode_without_crc
        crc = Zlib.crc32(data)

        encoder.write_int32(crc)
        encoder.write(data)
      end

      def ==(other)
        @key == other.key && @value == other.value && @attributes == other.attributes
      end

      def self.decode(decoder)
        crc = decoder.int32
        magic_byte = decoder.int8

        unless magic_byte == MAGIC_BYTE
          raise Kafka::Error, "Invalid magic byte: #{magic_byte}"
        end

        attributes = decoder.int8
        key = decoder.bytes
        value = decoder.bytes

        new(key: key, value: value, attributes: attributes)
      end

      private

      def encode_without_crc
        buffer = StringIO.new
        encoder = Encoder.new(buffer)

        encoder.write_int8(MAGIC_BYTE)
        encoder.write_int8(@attributes)
        encoder.write_bytes(@key)
        encoder.write_bytes(@value)

        buffer.string
      end
    end
  end
end
