module Kafka
  module Protocol
    class Record
      attr_reader :key, :value, :headers, :attributes, :bytesize
      attr_accessor :offset_delta, :timestamp_delta, :offset, :create_time, :is_control_record

      def initialize(
        key: nil,
        value:,
        headers: {},
        attributes: 0,
        offset_delta: 0,
        offset: 0,
        timestamp_delta: 0,
        create_time: Time.now,
        is_control_record: false
      )
        @key = key
        @value = value
        @headers = headers
        @attributes = attributes

        @offset_delta = offset_delta
        @offset = offset
        @timestamp_delta = timestamp_delta
        @create_time = create_time
        @is_control_record = is_control_record

        @bytesize = @key.to_s.bytesize + @value.to_s.bytesize
      end

      def encode(encoder)
        record_buffer = StringIO.new

        record_encoder = Encoder.new(record_buffer)

        record_encoder.write_int8(@attributes)
        record_encoder.write_varint(@timestamp_delta)
        record_encoder.write_varint(@offset_delta)

        record_encoder.write_varint_string(@key)
        record_encoder.write_varint_bytes(@value)

        record_encoder.write_varint_array(@headers.to_a) do |header_key, header_value|
          record_encoder.write_varint_string(header_key.to_s)
          record_encoder.write_varint_bytes(header_value.to_s)
        end

        encoder.write_varint_bytes(record_buffer.string)
      end

      def ==(other)
        offset_delta == other.offset_delta &&
          timestamp_delta == other.timestamp_delta &&
          offset == other.offset &&
          is_control_record == other.is_control_record
      end

      def self.decode(decoder)
        record_decoder = Decoder.from_string(decoder.varint_bytes)

        attributes = record_decoder.int8
        timestamp_delta = record_decoder.varint
        offset_delta = record_decoder.varint

        key = record_decoder.varint_string
        value = record_decoder.varint_bytes

        headers = {}
        record_decoder.varint_array do
          header_key = record_decoder.varint_string
          header_value = record_decoder.varint_bytes

          headers[header_key] = header_value
        end

        new(
          key: key,
          value: value,
          headers: headers,
          attributes: attributes,
          offset_delta: offset_delta,
          timestamp_delta: timestamp_delta
        )
      end
    end
  end
end
