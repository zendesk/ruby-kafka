module Kafka
  module Protocol
    class Record
      attr_reader :key, :value, :headers, :attributes, :offset_delta, :timestamp_delta
      attr_reader :bytesize, :create_time

      def initialize(
        key: nil,
        value:,
        headers: {},
        attributes: 0,
        offset_delta: 0,
        timestamp_delta: 0
      )
        @key = key
        @value = value
        @headers = headers
        @attributes = attributes
        @offset_delta = offset_delta
        @timestamp_delta = timestamp_delta

        @create_time = create_time
        @bytesize = @key.to_s.bytesize + @value.to_s.bytesize
      end

      def self.decode(decoder)
        record_length = decoder.varint
        record_decoder = Decoder.from_string(
          decoder.read(record_length),
          fixed_collection_size: false
        )

        attributes = record_decoder.int8
        timestamp_delta = record_decoder.varint
        offset_delta = record_decoder.varint

        key = record_decoder.string
        value = record_decoder.bytes

        headers = {}
        record_decoder.array do
          header_key = record_decoder.string
          header_value = record_decoder.bytes

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
