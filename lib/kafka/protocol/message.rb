require "stringio"
require "zlib"

module Kafka
  module Protocol

    # ## API Specification
    #
    #     Message => Crc MagicByte Attributes Timestamp Key Value
    #         Crc => int32
    #         MagicByte => int8
    #         Attributes => int8
    #         Timestamp => int64, in ms
    #         Key => bytes
    #         Value => bytes
    #
    class Message
      MAGIC_BYTE = 1

      attr_reader :key, :value, :codec_id, :offset

      attr_reader :bytesize, :create_time

      def initialize(value:, key: nil, create_time: Time.now, codec_id: 0, offset: -1)
        @key = key
        @value = value
        @codec_id = codec_id
        @offset = offset
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
        message_set = MessageSet.decode(message_set_decoder)

        # The contained messages need to have their offset corrected.
        messages = message_set.messages.each_with_index.map do |message, i|
          Message.new(
            offset: offset + i,
            value: message.value,
            key: message.key,
            create_time: message.create_time,
            codec_id: message.codec_id
          )
        end

        MessageSet.new(messages: messages)
      end

      def self.decode(decoder)
        offset = decoder.int64
        message_decoder = Decoder.from_string(decoder.bytes)

        crc = message_decoder.int32
        magic_byte = message_decoder.int8
        attributes = message_decoder.int8

        # The magic byte indicates the message format version. There are situations
        # where an old message format can be returned from a newer version of Kafka,
        # because old messages are not necessarily rewritten on upgrades.
        case magic_byte
        when 0
          # No timestamp in the pre-0.10 message format.
          timestamp = nil
        when 1
          timestamp = message_decoder.int64

          # If the timestamp is set to zero, it's because the message has been upgraded
          # from the Kafka 0.9 disk format to the Kafka 0.10 format. The former didn't
          # have a timestamp attribute, so we'll just set the timestamp to nil.
          timestamp = nil if timestamp.zero?
        else
          raise Kafka::Error, "Invalid magic byte: #{magic_byte}"
        end

        key = message_decoder.bytes
        value = message_decoder.bytes

        # The codec id is encoded in the three least significant bits of the
        # attributes.
        codec_id = attributes & 0b111

        # The timestamp will be nil if the message was written in the Kafka 0.9 log format.
        create_time = timestamp && Time.at(timestamp / 1000.0)

        new(key: key, value: value, codec_id: codec_id, offset: offset, create_time: create_time)
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
        encoder.write_int8(@codec_id)
        encoder.write_int64((@create_time.to_f * 1000).to_i)
        encoder.write_bytes(@key)
        encoder.write_bytes(@value)

        buffer.string
      end
    end
  end
end
