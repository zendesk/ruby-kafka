require 'bigdecimal'
require 'digest/crc32'
require 'kafka/protocol/record'

module Kafka
  module Protocol
    class RecordBatch
      MAGIC_BYTE = 2
      # The size of metadata before the real record data
      RECORD_BATCH_OVERHEAD = 49
      # Masks to extract information from attributes
      CODEC_ID_MASK = 0b00000111
      IN_TRANSACTION_MASK = 0b00010000
      IS_CONTROL_BATCH_MASK = 0b00100000
      TIMESTAMP_TYPE_MASK = 0b001000

      attr_reader :records, :first_offset, :first_timestamp, :partition_leader_epoch, :in_transaction, :is_control_batch, :last_offset_delta, :max_timestamp, :producer_id, :producer_epoch, :first_sequence

      attr_accessor :codec_id

      def initialize(
          records: [],
          first_offset: 0,
          first_timestamp: Time.now,
          partition_leader_epoch: 0,
          codec_id: 0,
          in_transaction: false,
          is_control_batch: false,
          last_offset_delta: 0,
          producer_id: -1,
          producer_epoch: 0,
          first_sequence: 0,
          max_timestamp: Time.now
      )
        @records = Array(records)
        @first_offset = first_offset
        @first_timestamp = first_timestamp
        @codec_id = codec_id

        # Records verification
        @last_offset_delta = last_offset_delta
        @max_timestamp = max_timestamp

        # Transaction information
        @producer_id = producer_id
        @producer_epoch = producer_epoch

        @first_sequence = first_sequence
        @partition_leader_epoch = partition_leader_epoch
        @in_transaction = in_transaction
        @is_control_batch = is_control_batch

        mark_control_record
      end

      def size
        @records.size
      end

      def last_offset
        @first_offset + @last_offset_delta
      end

      def attributes
        0x0000 | @codec_id |
          (@in_transaction ? IN_TRANSACTION_MASK : 0x0) |
          (@is_control_batch ? IS_CONTROL_BATCH_MASK : 0x0)
      end

      def encode(encoder)
        encoder.write_int64(@first_offset)

        record_batch_buffer = StringIO.new
        record_batch_encoder = Encoder.new(record_batch_buffer)

        record_batch_encoder.write_int32(@partition_leader_epoch)
        record_batch_encoder.write_int8(MAGIC_BYTE)

        body = encode_record_batch_body
        crc = ::Digest::CRC32c.checksum(body)

        record_batch_encoder.write_int32(crc)
        record_batch_encoder.write(body)

        encoder.write_bytes(record_batch_buffer.string)
      end

      def encode_record_batch_body
        buffer = StringIO.new
        encoder = Encoder.new(buffer)

        encoder.write_int16(attributes)
        encoder.write_int32(@last_offset_delta)
        encoder.write_int64((@first_timestamp.to_f * 1000).to_i)
        encoder.write_int64((@max_timestamp.to_f * 1000).to_i)

        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)
        encoder.write_int32(@first_sequence)

        encoder.write_int32(@records.length)

        records_array = encode_record_array
        if compressed?
          codec = Compression.find_codec_by_id(@codec_id)
          records_array = codec.compress(records_array)
        end
        encoder.write(records_array)

        buffer.string
      end

      def encode_record_array
        buffer = StringIO.new
        encoder = Encoder.new(buffer)
        @records.each do |record|
          record.encode(encoder)
        end
        buffer.string
      end

      def compressed?
        @codec_id != 0
      end

      def fulfill_relative_data
        first_record = records.min_by { |record| record.create_time }
        @first_timestamp = first_record.nil? ? Time.now : first_record.create_time

        last_record = records.max_by { |record| record.create_time }
        @max_timestamp = last_record.nil? ? Time.now : last_record.create_time

        records.each_with_index do |record, index|
          record.offset_delta = index
          record.timestamp_delta = ((record.create_time - first_timestamp) * 1000).to_i
        end
        @last_offset_delta = records.length - 1
      end

      def ==(other)
        records == other.records &&
          first_offset == other.first_offset &&
          partition_leader_epoch == other.partition_leader_epoch &&
          in_transaction == other.in_transaction &&
          is_control_batch == other.is_control_batch &&
          last_offset_delta == other.last_offset_delta &&
          producer_id == other.producer_id &&
          producer_epoch == other.producer_epoch &&
          first_sequence == other.first_sequence
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
        codec_id = attributes & CODEC_ID_MASK
        in_transaction = (attributes & IN_TRANSACTION_MASK) > 0
        is_control_batch = (attributes & IS_CONTROL_BATCH_MASK) > 0
        log_append_time = (attributes & TIMESTAMP_TYPE_MASK) != 0

        last_offset_delta = record_batch_decoder.int32
        first_timestamp = Time.at(record_batch_decoder.int64 / BigDecimal(1000))
        max_timestamp = Time.at(record_batch_decoder.int64 / BigDecimal(1000))

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
          record.offset = first_offset + record.offset_delta
          record.create_time = log_append_time && max_timestamp ? max_timestamp : first_timestamp + record.timestamp_delta / BigDecimal(1000)
          records_array << record
        end

        raise InsufficientDataMessage if records_array.length != records_array_length

        new(
          records: records_array,
          first_offset: first_offset,
          first_timestamp: first_timestamp,
          partition_leader_epoch: partition_leader_epoch,
          in_transaction: in_transaction,
          is_control_batch: is_control_batch,
          last_offset_delta: last_offset_delta,
          producer_id: producer_id,
          producer_epoch: producer_epoch,
          first_sequence: first_sequence,
          max_timestamp: max_timestamp
        )
      rescue EOFError
        raise InsufficientDataMessage, 'Partial trailing record detected!'
      end

      def mark_control_record
        if is_control_batch
          record = @records.first
          record.is_control_record = true unless record.nil?
        end
      end
    end
  end
end
