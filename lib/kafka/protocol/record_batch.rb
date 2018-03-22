require "kafka/protocol/record"

module Kafka
  module Protocol
    class RecordBatch
      # The size of metadata before the real record data
      RECORD_BATCH_OVERHEAD = 49

      attr_reader :records, :partition_leader_epoch, :in_traction, :has_control_message, :last_offset_delta, :max_timestamp, :producer_id, :producer_epoch, :first_sequence

      def initialize(
          records: [],
          partition_leader_epoch:,
          in_traction: false,
          has_control_message: false,
          last_offset_delta:,
          producer_id:,
          producer_epoch:,
          first_sequence:,
          max_timestamp:
      )
        @records = records

        # Records verification
        @last_offset_delta = last_offset_delta
        @max_timestamp = max_timestamp

        # Transaction information
        @producer_id = producer_id
        @producer_epoch = producer_epoch

        @first_sequence = first_sequence
        @partition_leader_epoch = partition_leader_epoch
        @in_traction = in_traction
        @has_control_message = has_control_message
      end

      def self.decode(decoder)
        first_offset = decoder.int64

        record_batch_raw = decoder.bytes
        record_batch_decoder = Decoder.from_string(record_batch_raw)

        partition_leader_epoch = record_batch_decoder.int32
        # Currently, the magic byte is used to distingush legacy MessageSet and
        # RecordBatch. Therefore, we don't care about magic byte here yet.
        _magic_byte = record_batch_decoder.int8
        _crc = record_batch_decoder.int32

        attributes = record_batch_decoder.int16
        codec_id = attributes & 0b111
        in_traction = (attributes & 0b10000) == 1
        has_control_message = (attributes & 0b100000) == 1

        last_offset_delta = record_batch_decoder.int32
        first_timestamp = record_batch_decoder.int64
        max_timestamp = record_batch_decoder.int64

        producer_id = record_batch_decoder.int64
        producer_epoch = record_batch_decoder.int16
        first_sequence = record_batch_decoder.int32

        records_array_length = record_batch_decoder.int32
        records_array_raw = record_batch_decoder.read(
          record_batch_raw.size - RECORD_BATCH_OVERHEAD
        )
        if codec_id != 0
          codec = Compression.find_codec_by_id(codec_id)
          records_array_raw = codec.decompress(records_array_raw)
        end

        records_array_decoder = Decoder.from_string(records_array_raw)
        records_array = []
        until records_array_decoder.eof?
          record = Record.decode(records_array_decoder)
          record.generate_absolute_offset(first_offset)
          record.generate_absolute_timestamp(first_timestamp)
          records_array << record
        end

        raise InsufficientDataMessage if records_array.length != records_array_length

        new(
          records: records_array,
          partition_leader_epoch: partition_leader_epoch,
          in_traction: in_traction,
          has_control_message: has_control_message,
          last_offset_delta: last_offset_delta,
          producer_id: producer_id,
          producer_epoch: producer_epoch,
          first_sequence: first_sequence,
          max_timestamp: max_timestamp
        )
      end
    end
  end
end
