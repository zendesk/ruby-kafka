require 'spec_helper'

describe Kafka::Protocol::RecordBatch do
  let(:empty_record_batch_bytes) do
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Record Batch Length
      0x0, 0x0, 0x0, 0x31,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x0,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0x0,
      # Last offset delta
      0x0, 0x0, 0x0, 0x0,
      # First timestamp
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Max timestamp
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Producer ID
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      # Producer epoch
      0x0, 0x0,
      # First sequence
      0x0, 0x0, 0x0, 0x0,
      # Number of records
      0x0, 0x0, 0x0, 0x0
    ]
  end

  let(:sample_record_batch) do
    Kafka::Protocol::RecordBatch.new(
      first_offset: 1,
      first_timestamp: Time.at(1521656000),
      partition_leader_epoch: 2,
      codec_id: codec_id,
      has_control_message: false,
      last_offset_delta: 3,
      max_timestamp: Time.at(1521658000),
      producer_id: 123456,
      producer_epoch: 2,
      first_sequence: 0,
      records: [
        Kafka::Protocol::Record.new(
          attributes:     1,
          timestamp_delta: 1000,
          offset_delta:    1,
          key: 'hello',
          value: 'world',
          headers: {
            'a' => '12'
          }
        ),
        Kafka::Protocol::Record.new(
          attributes: 2,
          timestamp_delta: 2000,
          offset_delta: 2,
          key: 'ruby',
          value: 'kafka',
          headers: {
            'b' => '34'
          }
        )
      ]
    )
  end

  let(:sample_record_batch_metadata_bytes) {
    [
      # Last offset delta
      0x0, 0x0, 0x0, 0x3,
      # First timestamp
      0x0, 0x0, 0x1, 0x62, 0x49, 0xc3, 0xee, 0x0,
      # Max timestamp
      0x0, 0x0, 0x1, 0x62, 0x49, 0xe2, 0x72, 0x80,
      # Producer ID
      0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xe2, 0x40,
      # Producer epoch
      0x0, 0x2,
      # First sequence
      0x0, 0x0, 0x0, 0x0,
      # Number of records
      0x0, 0x0, 0x0, 0x2
    ]
  }

  let(:record_1_bytes) {
    [
      # Size
      0x2c,
      # Attributes
      0x1,
      # Timestamp delta
      0xd0, 0xf,
      # Offset delta
      0x2,
      # Key
      0xa,
      0x68, 0x65, 0x6c, 0x6c, 0x6f,
      # Value
      0xa,
      0x77, 0x6f, 0x72, 0x6c, 0x64,
      # Header
      0x2,
      0x2, 0x61,
      0x4, 0x31, 0x32,
    ]
  }

  let(:record_2_bytes) {
    [
      # Size
      0x2a,
      # Attributes
      0x2,
      # Timestamp delta
      0xa0, 0x1f,
      # Offset delta
      0x4,
      # Key
      0x8,
      0x72, 0x75, 0x62, 0x79,
      # Value
      0xa,
      0x6b, 0x61, 0x66, 0x6b, 0x61,
      # Header
      0x2,
      0x2, 0x62,
      0x4, 0x33, 0x34
    ]
  }

  let(:sample_record_batch_uncompressed_bytes) do
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
      # Record Batch Length
      0x0, 0x0, 0x0, 0x5e,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x2,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0x0,
      sample_record_batch_metadata_bytes,
      record_1_bytes,
      record_2_bytes
    ].flatten
  end

  let(:sample_record_batch_gzip_bytes) do
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
      # Record Batch Length
      0x0, 0x0, 0x0, 0x72,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x2,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0x1,
      sample_record_batch_metadata_bytes,
      Kafka::GzipCodec.new.compress(
        (record_1_bytes + record_2_bytes).pack("C*")
      ).bytes
    ].flatten
  end

  let(:sample_record_batch_snappy_bytes) do
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
      # Record Batch Length
      0x0, 0x0, 0x0, 0x60,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x2,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0x2,
      sample_record_batch_metadata_bytes,
      Kafka::SnappyCodec.new.compress(
        (record_1_bytes + record_2_bytes).pack("C*")
      ).bytes
    ].flatten
  end

  let(:sample_record_batch_lz4_bytes) do
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
      # Record Batch Length
      0x0, 0x0, 0x0, 0x71,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x2,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0x3,
      sample_record_batch_metadata_bytes,
      Kafka::LZ4Codec.new.compress(
        (record_1_bytes + record_2_bytes).pack("C*")
      ).bytes
    ].flatten
  end

  context '#encode' do
    let(:buffer) { StringIO.new }
    let(:encoder) { Kafka::Protocol::Encoder.new(buffer) }

    context 'Empty record batch' do
      let(:record_batch) do
        Kafka::Protocol::RecordBatch.new(
          first_timestamp: Time.at(0),
          max_timestamp: Time.at(0)
        )
      end

      it 'encodes an empty batch with no records' do
        record_batch.encode(encoder)
        expect(strip_crc(buffer.string.bytes)).to eql empty_record_batch_bytes
      end
    end

    context 'Uncompressed record batch' do
      let(:codec_id) { 0 }

      it 'encodes the record batch without compressing' do
        sample_record_batch.encode(encoder)
        expect(strip_crc(buffer.string.bytes)).to eql sample_record_batch_uncompressed_bytes
      end
    end

    context 'Compress with GZIP' do
      let(:codec_id) { 1 }

      it 'encodes the record batch using gzip compressor' do
        sample_record_batch.encode(encoder)
        expect(strip_crc(buffer.string.bytes)).to eql sample_record_batch_gzip_bytes
      end
    end

    context 'Compress with Snappy' do
      let(:codec_id) { 2 }

      it 'encodes the record batch using snappy compressor' do
        sample_record_batch.encode(encoder)
        expect(strip_crc(buffer.string.bytes)).to eql sample_record_batch_snappy_bytes
      end
    end

    context 'Compress with LZ4' do
      let(:codec_id) { 3 }

      it 'encodes the record batch using snappy compressor' do
        sample_record_batch.encode(encoder)
        expect(strip_crc(buffer.string.bytes)).to eql sample_record_batch_lz4_bytes
      end
    end
  end

  context '.decode' do
    context 'Empty record batch' do
      let(:decoder) { Kafka::Protocol::Decoder.new(byte_array_to_io(empty_record_batch_bytes)) }

      it 'returns record batch with no records' do
        record_batch = Kafka::Protocol::RecordBatch.decode(decoder)
        expect(record_batch.records).to eql([])

        # Records verification
        expect(record_batch.last_offset_delta).to eql 0
        expect(record_batch.max_timestamp.to_i).to eql 0

        # Transaction information
        expect(record_batch.producer_id).to eql -1
        expect(record_batch.producer_epoch).to eql 0

        expect(record_batch.first_sequence).to eql 0
        expect(record_batch.partition_leader_epoch).to eql 0
        expect(record_batch.in_traction).to eql false
        expect(record_batch.has_control_message).to eql false
      end
    end

    context 'Uncompressed records' do
      let(:decoder) do
        Kafka::Protocol::Decoder.new(byte_array_to_io(sample_record_batch_uncompressed_bytes))
      end

      it 'decodes records without decompressing' do
        record_batch = Kafka::Protocol::RecordBatch.decode(decoder)
        expect_matched_batch_metadata(record_batch)
        expect_matched_records(record_batch.records)
      end
    end

    context 'Compress with GZIP' do
      let(:decoder) do
        Kafka::Protocol::Decoder.new(byte_array_to_io(sample_record_batch_gzip_bytes))
      end

      it 'decodes records with GZIP decompressor' do
        record_batch = Kafka::Protocol::RecordBatch.decode(decoder)
        expect_matched_batch_metadata(record_batch)
        expect_matched_records(record_batch.records)
      end
    end

    context 'Compress with Snappy' do
      let(:decoder) do
        Kafka::Protocol::Decoder.new(byte_array_to_io(sample_record_batch_snappy_bytes))
      end

      it 'decodes records with Snappy decompressor' do
        record_batch = Kafka::Protocol::RecordBatch.decode(decoder)
        expect_matched_batch_metadata(record_batch)
        expect_matched_records(record_batch.records)
      end
    end

    context 'Compress with LZ4' do
      let(:decoder) do
        Kafka::Protocol::Decoder.new(byte_array_to_io(sample_record_batch_lz4_bytes))
      end

      it 'decodes records with LZ4 decompressor' do
        record_batch = Kafka::Protocol::RecordBatch.decode(decoder)
        expect_matched_batch_metadata(record_batch)
        expect_matched_records(record_batch.records)
      end
    end
  end
end

def byte_array_to_io(bytes)
  str = bytes.map { |byte| [byte].pack("C") }.join("")
  StringIO.new(str)
end

def expect_matched_batch_metadata(record_batch)
  # Records verification
  expect(record_batch.last_offset_delta).to eql 3
  expect(record_batch.first_offset).to eql 1
  expect(record_batch.first_timestamp.to_i).to eql 1521656000
  expect(record_batch.max_timestamp.to_i).to eql 1521658000

  # Transaction information
  expect(record_batch.producer_id).to eql 123456
  expect(record_batch.producer_epoch).to eql 2

  expect(record_batch.first_sequence).to eql 0
  expect(record_batch.partition_leader_epoch).to eql 2
  expect(record_batch.in_traction).to eql false
  expect(record_batch.has_control_message).to eql false
end

def expect_matched_records(records)
  expect(records.length).to eql 2

  record_1 = records.first
  expect(record_1.attributes).to eql(1)

  expect(record_1.timestamp_delta).to eql(1000)
  expect(record_1.create_time.to_i).to eql(1521657000)

  expect(record_1.offset_delta).to eql(1)
  expect(record_1.offset).to eql(2)

  expect(record_1.key).to eql('hello')
  expect(record_1.value).to eql('world')
  expect(record_1.headers).to eql('a' => '12')

  record_2 = records.last
  expect(record_2.attributes).to eql(2)
  expect(record_2.timestamp_delta).to eql(2000)
  expect(record_2.create_time.to_i).to eql(1521658000)
  expect(record_2.offset_delta).to eql(2)
  expect(record_2.offset).to eql(3)

  expect(record_2.key).to eql('ruby')
  expect(record_2.value).to eql('kafka')
  expect(record_2.headers).to eql('b' => '34')
end

# Compression usually has random factors. Therefore, the content and crc change
# all the time. It is useless to compare crc
def strip_crc(bytes)
  bytes[17] = 0x0
  bytes[18] = 0x0
  bytes[19] = 0x0
  bytes[20] = 0x0
  bytes
end
