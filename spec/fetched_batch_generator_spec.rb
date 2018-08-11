# frozen_string_literal: true

describe Kafka::FetchedBatchGenerator do
  let(:logger) { LOGGER }
  let(:generator) { described_class.new('Hello', fetched_partition, logger: logger) }
  let(:message_1) { Kafka::Protocol::Message.new(value: 'Hello') }
  let(:message_2) { Kafka::Protocol::Message.new(value: 'World') }
  let(:message_3) { Kafka::Protocol::Message.new(value: 'Bye') }

  context 'empty partition' do
    let(:fetched_partition) do
      Kafka::Protocol::FetchResponse::FetchedPartition.new(
        partition: 0,
        error_code: 0,
        highwater_mark_offset: 1,
        last_stable_offset: 1,
        aborted_transactions: [],
        messages: []
      )
    end

    it 'returns empty array' do
      batch = generator.generate
      expect(batch.topic).to eql('Hello')
      expect(batch.partition).to eql(0)
      expect(batch.last_offset).to eql(nil)
      expect(batch.highwater_mark_offset).to eql(1)
      expect(batch.messages).to eql([])
    end
  end

  context 'legacy message set' do
    let(:fetched_partition) do
      Kafka::Protocol::FetchResponse::FetchedPartition.new(
        partition: 0,
        error_code: 0,
        highwater_mark_offset: 1,
        last_stable_offset: 1,
        aborted_transactions: [],
        messages: [
          Kafka::Protocol::MessageSet.new(
            messages: [
              message_1,
              message_2
            ]
          ),
          Kafka::Protocol::MessageSet.new(
            messages: [
              message_3
            ]
          )
        ]
      )
    end

    it 'returns flatten messages' do
      batch = generator.generate
      expect(batch.topic).to eql('Hello')
      expect(batch.partition).to eql(0)
      expect(batch.highwater_mark_offset).to eql(1)

      expect(batch.messages.length).to eql(3)

      expect_fetched_message_eql(batch.messages[0], 'Hello', 0, message_1)
      expect_fetched_message_eql(batch.messages[1], 'Hello', 0, message_2)
      expect_fetched_message_eql(batch.messages[2], 'Hello', 0, message_3)
    end
  end

  context 'record batch format' do
    let(:record_1) { Kafka::Protocol::Record.new(value: 'Hello') }
    let(:record_2) { Kafka::Protocol::Record.new(value: 'World') }
    let(:record_3) { Kafka::Protocol::Record.new(value: 'Bye') }

    context 'no control record' do
      let(:fetched_partition) do
        Kafka::Protocol::FetchResponse::FetchedPartition.new(
          partition: 0,
          error_code: 0,
          highwater_mark_offset: 1,
          last_stable_offset: 1,
          aborted_transactions: [],
          messages: [
            Kafka::Protocol::RecordBatch.new(
              first_offset: 9,
              last_offset_delta: 1,
              records: [
                record_1, record_2
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              first_offset: 11,
              last_offset_delta: 1,
              records: [
                record_3
              ]
            )
          ]
        )
      end

      it 'returns flatten records' do
        batch = generator.generate
        expect(batch.topic).to eql('Hello')
        expect(batch.partition).to eql(0)
        expect(batch.last_offset).to eql(12)
        expect(batch.highwater_mark_offset).to eql(1)

        expect(batch.messages.length).to eql(3)

        expect_fetched_message_eql(batch.messages[0], 'Hello', 0, record_1)
        expect_fetched_message_eql(batch.messages[1], 'Hello', 0, record_2)
        expect_fetched_message_eql(batch.messages[2], 'Hello', 0, record_3)
      end
    end

    context 'including committed control records' do
      let(:fetched_partition) do
        Kafka::Protocol::FetchResponse::FetchedPartition.new(
          partition: 0,
          error_code: 0,
          highwater_mark_offset: 1,
          last_stable_offset: 1,
          aborted_transactions: [],
          messages: [
            Kafka::Protocol::RecordBatch.new(
              first_offset: 7,
              last_offset_delta: 2,
              records: [
                record_1,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                ),
                record_2
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              first_offset: 10,
              last_offset_delta: 2,
              records: [
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                ),
                record_3,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                ),
              ]
            )
          ]
        )
      end

      it 'ignores control records' do
        batch = generator.generate
        expect(batch.topic).to eql('Hello')
        expect(batch.partition).to eql(0)
        expect(batch.last_offset).to eql(12)
        expect(batch.highwater_mark_offset).to eql(1)

        expect(batch.messages.length).to eql(3)

        expect_fetched_message_eql(batch.messages[0], 'Hello', 0, record_1)
        expect_fetched_message_eql(batch.messages[1], 'Hello', 0, record_2)
        expect_fetched_message_eql(batch.messages[2], 'Hello', 0, record_3)
      end
    end

    context 'including committed control batch' do
      let(:fetched_partition) do
        Kafka::Protocol::FetchResponse::FetchedPartition.new(
          partition: 0,
          error_code: 0,
          highwater_mark_offset: 1,
          last_stable_offset: 1,
          aborted_transactions: [],
          messages: [
            Kafka::Protocol::RecordBatch.new(
              first_offset: 7,
              last_offset_delta: 0,
              records: [
                record_1,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                ),
                record_2
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              first_offset: 8,
              last_offset_delta: 0,
              is_control_batch: true,
              records: [
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                )
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              first_offset: 9,
              last_offset_delta: 0,
              records: [
                record_3,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                )
              ]
            )
          ]
        )
      end

      it 'ignores control batches and control records' do
        batch = generator.generate
        expect(batch.topic).to eql('Hello')
        expect(batch.partition).to eql(0)
        expect(batch.last_offset).to eql(9)
        expect(batch.highwater_mark_offset).to eql(1)

        expect(batch.messages.length).to eql(3)

        expect_fetched_message_eql(batch.messages[0], 'Hello', 0, record_1)
        expect_fetched_message_eql(batch.messages[1], 'Hello', 0, record_2)
        expect_fetched_message_eql(batch.messages[2], 'Hello', 0, record_3)
      end
    end

    context 'including aborted control records and aborted transactions' do
      let(:record_1) { Kafka::Protocol::Record.new(value: 'Hello', offset: 12) }
      let(:record_2) { Kafka::Protocol::Record.new(value: 'World', offset: 13) }
      let(:record_3) { Kafka::Protocol::Record.new(value: 'Bye', offset: 14) }

      let(:fetched_partition) do
        Kafka::Protocol::FetchResponse::FetchedPartition.new(
          partition: 0,
          error_code: 0,
          highwater_mark_offset: 1,
          last_stable_offset: 1,
          aborted_transactions: [
            Kafka::Protocol::FetchResponse::AbortedTransaction.new(
              producer_id: 5,
              first_offset: 16
            ),
            Kafka::Protocol::FetchResponse::AbortedTransaction.new(
              producer_id: 5,
              first_offset: 10
            ),
            Kafka::Protocol::FetchResponse::AbortedTransaction.new(
              producer_id: 7,
              first_offset: 15
            )
          ],
          messages: [
            Kafka::Protocol::RecordBatch.new(
              producer_id: 5,
              in_transaction: true,
              first_offset: 10,
              last_offset_delta: 1,
              records: [
                Kafka::Protocol::Record.new(value: 'Aborted 1', offset: 10),
                Kafka::Protocol::Record.new(value: 'Aborted 2', offset: 11)
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 5,
              in_transaction: true,
              is_control_batch: true,
              records: [
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x00",
                  value: nil
                )
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 5,
              in_transaction: true,
              first_offset: 12,
              last_offset_delta: 1,
              records: [
                record_1,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                ),
                record_2
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 6,
              in_transaction: true,
              first_offset: 14,
              last_offset_delta: 0,
              records: [
                record_3,
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x01",
                  value: nil
                )
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 7,
              in_transaction: true,
              first_offset: 15,
              last_offset_delta: 0,
              records: [
                Kafka::Protocol::Record.new(value: 'Aborted 3', offset: 15)
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 7,
              in_transaction: true,
              is_control_batch: true,
              records: [
                Kafka::Protocol::Record.new(
                  is_control_record: true,
                  key: "\x00\x00\x00\x00",
                  value: nil
                )
              ]
            ),
            Kafka::Protocol::RecordBatch.new(
              producer_id: 5,
              in_transaction: true,
              first_offset: 16,
              last_offset_delta: 0,
              records: [
                Kafka::Protocol::Record.new(value: 'Aborted 4', offset: 16)
              ]
            ),
          ]
        )
      end

      it 'ignores control records and aborted records' do
        batch = generator.generate
        expect(batch.topic).to eql('Hello')
        expect(batch.partition).to eql(0)
        expect(batch.last_offset).to eql(16)
        expect(batch.highwater_mark_offset).to eql(1)

        expect(batch.messages.length).to eql(3)

        expect_fetched_message_eql(batch.messages[0], 'Hello', 0, record_1)
        expect_fetched_message_eql(batch.messages[1], 'Hello', 0, record_2)
        expect_fetched_message_eql(batch.messages[2], 'Hello', 0, record_3)
      end
    end
  end
end

def expect_fetched_message_eql(fetched_message, topic, partition, message)
  expect(fetched_message.topic).to eql(topic)
  expect(fetched_message.partition).to eql(partition)

  expect(fetched_message.value).to eql(message.value)
  expect(fetched_message.key).to eql(message.key)
  expect(fetched_message.offset).to eql(message.offset)
  expect(fetched_message.create_time).to eql(message.create_time)

  if message.is_a?(Kafka::Protocol::Record)
    expect(fetched_message.headers).to eql(message.headers)
    expect(fetched_message.is_control_record).to eql(message.is_control_record)
  end
end
