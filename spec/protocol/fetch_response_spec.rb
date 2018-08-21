require 'spec_helper'

describe Kafka::Protocol::FetchResponse do
  let(:headers) do
    [
      # Throttle time
      0x00, 0x00, 0x00, 0x00,
      # Number of topics
      0x00, 0x00, 0x00, 0x01,
      # Topic name: hello
      0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
      # Number of partitions
      0x00, 0x00, 0x00, 0x01,
      # Partition 0
      0x00, 0x00, 0x00, 0x00,
      # Error
      0x00, 0x00,
      # High Watermark Offset
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      # Last Stable Offset
      0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
      # Aborted transactions
      0x00, 0x00, 0x00, 0x00
    ]
  end

  let(:first_record) {
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Record Batch Size
      0x0, 0x0, 0x0, 0x43,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x0,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0b00000000,
      # Last offset delta
      0x0, 0x0, 0x0, 0x3,
      # First timestamp
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Max timestamp
      0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Producer ID
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Producer epoch
      0x0, 0x0,
      # First sequence
      0x0, 0x0, 0x0, 0x0,
      # Number of records
      0x0, 0x0, 0x0, 0x1,
      # Size
      0x22,
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
      0x0
    ]
  }

  let(:second_record) {
    [
      # First offset
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Record Batch Size
      0x0, 0x0, 0x0, 0x43,
      # Partition Leader Epoch
      0x0, 0x0, 0x0, 0x0,
      # Magic byte
      0x2,
      # CRC
      0x0, 0x0, 0x0, 0x0,
      # Attributes
      0x0, 0b00000000,
      # Last offset delta
      0x0, 0x0, 0x0, 0x3,
      # First timestamp
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Max timestamp
      0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Producer ID
      0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
      # Producer epoch
      0x0, 0x0,
      # First sequence
      0x0, 0x0, 0x0, 0x0,
      # Number of records
      0x0, 0x0, 0x0, 0x1,
      # Size
      0x22,
      # Attributes
      0x1,
      # Timestamp delta
      0xd0, 0xf,
      # Offset delta
      0x2,
      # Key
      0xa,
      0x68, 0x69, 0x69, 0x69, 0x69,
      # Value
      0xa,
      0x62, 0x79, 0x65, 0x65, 0x65,
      # Header
      0x0
    ]
  }

  let(:message) {
    [
      # Offset
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x13,
      # CRC
      0x00, 0x00, 0x00, 0x00,
      # Magic bytes
      0x00,
      # Attributes
      0x00,
      # Key
      0xFF, 0xFF, 0xFF, 0xFF,
      # Value
      0x0, 0x0, 0x0, 0x5,
      0x68, 0x65, 0x6c, 0x6c, 0x6f
    ]
  }

  describe '.decode' do
    describe 'Record Batch format' do
      context 'single record batch' do
        let(:response) do
          [
            headers,
            # Record Batch Byte Size
            0x0, 0x0, 0x0, 0x4f,
            first_record
          ].flatten
        end
        let(:decoder) do
          ::Kafka::Protocol::Decoder.from_string(
            response.pack("C*")
          )
        end

        it 'decodes the fetch response' do
          response = ::Kafka::Protocol::FetchResponse.decode(decoder)
          expect(response.topics.length).to eql(1)
          topic = response.topics.first

          expect(topic.partitions.length).to eql(1)
          partition = topic.partitions.first

          expect(partition.partition).to eql(0)
          expect(partition.error_code).to eql(0)
          expect(partition.aborted_transactions).to eql([])
          expect(partition.messages.length).to eql(1)

          record_batch = partition.messages.first
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records.first.key).to eql('hello')
          expect(record_batch.records.first.value).to eql('world')
        end
      end

      context 'multiple full record batch' do
        let(:response) do
          [
            headers,
            # Record Batches Byte Size
            0x0, 0x0, 0x0, 0x9e,
            first_record,
            second_record
          ].flatten
        end
        let(:decoder) do
          ::Kafka::Protocol::Decoder.from_string(
            response.pack("C*")
          )
        end

        it 'recognizes 2 record batches' do
          response = ::Kafka::Protocol::FetchResponse.decode(decoder)
          expect(response.topics.length).to eql(1)
          topic = response.topics.first

          expect(topic.partitions.length).to eql(1)
          partition = topic.partitions.first

          expect(partition.partition).to eql(0)
          expect(partition.error_code).to eql(0)
          expect(partition.aborted_transactions).to eql([])
          expect(partition.messages.length).to eql(2)

          record_batch = partition.messages.first
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hello')
          expect(record_batch.records[0].value).to eql('world')

          record_batch = partition.messages.last
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hiiii')
          expect(record_batch.records[0].value).to eql('byeee')
        end
      end

      context 'partial record batch' do
        let(:response) do
          [
            headers,
            # Record Batches Byte Size
            0x0, 0x0, 0x0, 0xa0,
            first_record,
            second_record,
            0x01, 0x02
          ].flatten
        end

        let(:decoder) do
          ::Kafka::Protocol::Decoder.from_string(
            response.pack("C*")
          )
        end

        it 'ignores the trailing incompleted record' do
          response = ::Kafka::Protocol::FetchResponse.decode(decoder)
          expect(response.topics.length).to eql(1)
          topic = response.topics.first

          expect(topic.partitions.length).to eql(1)
          partition = topic.partitions.first

          expect(partition.partition).to eql(0)
          expect(partition.error_code).to eql(0)
          expect(partition.aborted_transactions).to eql([])
          expect(partition.messages.length).to eql(2)

          record_batch = partition.messages.first
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hello')
          expect(record_batch.records[0].value).to eql('world')

          record_batch = partition.messages.last
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hiiii')
          expect(record_batch.records[0].value).to eql('byeee')
        end
      end

      context 'multiple partitions' do
        let(:response) do
          [
            # Throttle time
            0x00, 0x00, 0x00, 0x00,
            # Number of topics
            0x00, 0x00, 0x00, 0x01,
            # Topic name: hello
            0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
            # Number of partitions
            0x00, 0x00, 0x00, 0x02,

            # Partition 0
            0x00, 0x00, 0x00, 0x00,
            # Error
            0x00, 0x00,
            # High Watermark Offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            # Last Stable Offset
            0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            # Aborted transactions
            0x00, 0x00, 0x00, 0x00,
            # Record Batches Byte Size
            0x0, 0x0, 0x0, 0x50,
            first_record,
            # Partial record
            0x1,

            # Partition 1
            0x00, 0x00, 0x00, 0x01,
            # Error
            0x00, 0x00,
            # High Watermark Offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            # Last Stable Offset
            0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
            # Aborted transactions
            0x00, 0x00, 0x00, 0x00,
            # Record Batches Byte Size
            0x0, 0x0, 0x0, 0x52,
            second_record,
            # Partial record
            0x1, 0x2, 0x3,
          ].flatten
        end

        let(:decoder) do
          ::Kafka::Protocol::Decoder.from_string(
            response.pack("C*")
          )
        end

        it 'decodes completed record batch in both partitions' do
          response = ::Kafka::Protocol::FetchResponse.decode(decoder)
          expect(response.topics.length).to eql(1)
          topic = response.topics.first

          expect(topic.partitions.length).to eql(2)
          partition = topic.partitions[0]

          expect(partition.partition).to eql(0)
          expect(partition.error_code).to eql(0)
          expect(partition.aborted_transactions).to eql([])
          expect(partition.messages.length).to eql(1)

          record_batch = partition.messages.first
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hello')
          expect(record_batch.records[0].value).to eql('world')

          partition = topic.partitions[1]

          expect(partition.partition).to eql(1)
          expect(partition.error_code).to eql(0)
          expect(partition.aborted_transactions).to eql([])
          expect(partition.messages.length).to eql(1)

          record_batch = partition.messages.first
          expect(record_batch.records.length).to eql(1)
          expect(record_batch.records[0].key).to eql('hiiii')
          expect(record_batch.records[0].value).to eql('byeee')
        end
      end
    end

    describe 'Legacy MessageSet' do
      let(:response) do
        [
          headers,
          0x00, 0x00, 0x00, 0x1f,
          message
        ].flatten
      end
      let(:decoder) do
        ::Kafka::Protocol::Decoder.from_string(
          response.pack("C*")
        )
      end

      it 'decodes the fetch response' do
        response = ::Kafka::Protocol::FetchResponse.decode(decoder)

        expect(response.topics.length).to eql(1)
        topic = response.topics.first

        expect(topic.partitions.length).to eql(1)
        partition = topic.partitions[0]

        expect(partition.partition).to eql(0)
        expect(partition.error_code).to eql(0)
        expect(partition.aborted_transactions).to eql([])
        expect(partition.messages.length).to eql(1)

        expect(partition.messages.first).to be_a(::Kafka::Protocol::MessageSet)

        message_set = partition.messages.first
        expect(message_set.messages[0]).to be_a(::Kafka::Protocol::Message)
        expect(message_set.messages[0].value).to eql('hello')
      end
    end
  end
end
