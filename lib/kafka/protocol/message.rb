# frozen_string_literal: true

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

      # @return [Array<Kafka::Protocol::Message>]
      def decompress
        codec = Compression.find_codec_by_id(@codec_id)

        # For some weird reason we need to cut out the first 20 bytes.
        data = codec.decompress(value)
        message_set_decoder = Decoder.from_string(data)
        message_set = MessageSet.decode(message_set_decoder)

        correct_offsets(message_set.messages)
      end

      def self.decode(decoder)
        offset = decoder.int64
        message_decoder = Decoder.from_string(decoder.bytes)

        _crc = message_decoder.int32
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

      # Ensure the backward compatibility of Message format from Kafka 0.11.x
      def is_control_record
        false
      end

      def headers
        {}
      end

      private

      # Offsets may be relative with regards to wrapped message offset, but there are special cases.
      #
      # Cases when client will receive corrected offsets:
      #   - When fetch request is version 0, kafka will correct relative offset on broker side before replying fetch response
      #   - When messages is stored in 0.9 format on disk (broker configured to do so).
      #
      # All other cases, compressed inner messages should have relative offset, with below attributes:
      #   - The container message should have the 'real' offset
      #   - The container message's offset should be the 'real' offset of the last message in the compressed batch
      def correct_offsets(messages)
        max_relative_offset = messages.last.offset

        # The offsets are already correct, do nothing.
        return messages if max_relative_offset == offset

        # The contained messages have relative offsets, and needs to be corrected.
        base_offset = offset - max_relative_offset

        messages.map do |message|
          Message.new(
            offset: message.offset + base_offset,
            value: message.value,
            key: message.key,
            create_time: message.create_time,
            codec_id: message.codec_id
          )
        end
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
        encoder.write_int64((@create_time.to_f * 1000).to_i)
        encoder.write_bytes(@key)
        encoder.write_bytes(@value)

        buffer.string
      end
    end
  end
end
