require "zlib"

module Kafka
  module Protocol

    # == API Specification
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
