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

      attr_reader :key, :value, :attributes, :offset

      attr_reader :bytesize

      def initialize(value:, key: nil, attributes: 0, offset: -1)
        @key = key
        @value = value
        @attributes = attributes
        @offset = offset

        @bytesize = @key.to_s.bytesize + @value.to_s.bytesize
      end

      def encode(encoder)
        data = encode_with_crc

        encoder.write_int64(offset)
        encoder.write_bytes(data)
      end

      def ==(other)
        @key == other.key &&
          @value == other.value &&
          @attributes == other.attributes &&
          @offset == other.offset
      end

      def compressed?
        @attributes != 0
      end

      # @return [Kafka::Protocol::MessageSet]
      def decompress
        codec = Compression.find_codec_by_id(@attributes)

        # For some weird reason we need to cut out the first 20 bytes.
        data = codec.decompress(value)
        message_set_decoder = Decoder.from_string(data)

        MessageSet.decode(message_set_decoder)
      end

      def self.decode(decoder)
        offset = decoder.int64
        message_decoder = Decoder.from_string(decoder.bytes)

        crc = message_decoder.int32
        magic_byte = message_decoder.int8

        unless magic_byte == MAGIC_BYTE
          raise Kafka::Error, "Invalid magic byte: #{magic_byte}"
        end

        attributes = message_decoder.int8
        key = message_decoder.bytes
        value = message_decoder.bytes

        new(key: key, value: value, attributes: attributes, offset: offset)
      end

      private

      def encode_with_crc
        buffer = StringIO.new
        encoder = Encoder.new(buffer)

        data = encode_without_crc
        crc = Zlib.crc32(data)

        encoder.write_int32(crc)
        encoder.write(data)

        buffer.string
      end

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
