require "stringio"
require "zlib"

module Kafka
  module Protocol

    # ## API Specification
    #
    #   v1
    #   Message => Crc MagicByte Attributes Key Value
    #     Crc => int32
    #     MagicByte => int8
    #     Attributes => int8
    #     Key => bytes
    #     Value => bytes
    #
    #   v2 (supported since 0.10.0)
    #   Message => Crc MagicByte Attributes Timestamp Key Value
    #     Crc => int32
    #     MagicByte => int8
    #     Attributes => int8
    #     Timestamp => int64
    #     Key => bytes
    #     Value => bytes
    #
    class Message
      V1_MAGIC_BYTE = 0
      V2_MAGIC_BYTE = 1
      MAGIC_BYTE = V2_MAGIC_BYTE

      attr_reader :key, :value, :timestamp, :codec_id, :offset

      attr_reader :bytesize, :create_time

      def initialize(value:, key: nil, create_time: Time.now, codec_id: 0, offset: -1)
        @key = key
        @value = value
        @codec_id = codec_id
        @offset = offset
        @timestamp = (create_time.to_f*1000.0).to_i
        @create_time = create_time

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
          @timestamp == other.timestamp &&
          @codec_id == other.codec_id &&
          @offset == other.offset
      end

      def compressed?
        @codec_id != 0
      end

      # @return [Kafka::Protocol::MessageSet]
      def decompress
        codec = Compression.find_codec_by_id(@codec_id)

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

        case magic_byte
        when V1_MAGIC_BYTE
          self.v1_decode(offset, message_decoder)
        when V2_MAGIC_BYTE
          self.v2_decode(offset, message_decoder)
        else
          raise Kafka::Error, "Invalid magic byte: #{magic_byte}"
        end
      end

      private

      def self.v1_decode(offset, message_decoder)
        attributes = message_decoder.int8
        key = message_decoder.bytes
        value = message_decoder.bytes

        # The codec id is encoded in the three least significant bits of the
        # attributes.
        codec_id = attributes & 0b111

        new(key: key, value: value, codec_id: codec_id, offset: offset)
      end

      def self.v2_decode(offset, message_decoder)
        attributes = message_decoder.int8
        timestamp = Time.at(message_decoder.int64/1000)
        key = message_decoder.bytes
        value = message_decoder.bytes

        # The codec id is encoded in the three least significant bits of the
        # attributes.
        codec_id = attributes & 0b111

        new(key: key, value: value, create_time: timestamp, codec_id: codec_id, offset: offset)
      end

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
        encoder.write_int8(@codec_id)
        encoder.write_int64(@timestamp)
        encoder.write_bytes(@key)
        encoder.write_bytes(@value)

        buffer.string
      end
    end
  end
end
