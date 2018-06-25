# frozen_string_literal: true

describe Kafka::ProduceOperation do
  let(:cluster) { double(:cluster) }
  let(:transaction_manager) { double(:transaction_manager) }
  let(:compressor) { double(:compressor) }
  let(:buffer) { Kafka::MessageBuffer.new }
  let(:logger) { LOGGER }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }

  let(:broker_1) { double(:broker) }
  let(:broker_2) { double(:broker) }
  let(:broker_3) { double(:broker) }

  before do
    allow(cluster).to receive(:get_leader).with('hello', 0).and_return(broker_1)
    allow(cluster).to receive(:get_leader).with('hello', 1).and_return(broker_2)
    allow(cluster).to receive(:get_leader).with('hi', 0).and_return(broker_3)
    allow(cluster).to receive(:get_leader).with('hi', 1).and_return(broker_3)
    allow(cluster).to receive(:get_leader).with('bye', 0).and_return(broker_3)
  end

  context 'normal message production' do
    let(:operation) {
      Kafka::ProduceOperation.new(
        cluster: cluster,
        transaction_manager: transaction_manager,
        buffer: buffer,
        compressor: compressor,
        logger: logger,
        instrumenter: instrumenter,
        required_acks: -1,
        ack_timeout: 5
      )
    }

    before do
      allow(transaction_manager).to receive(:idempotent?).and_return(false)
      allow(transaction_manager).to receive(:transactional?).and_return(false)
      allow(transaction_manager).to receive(:next_sequence_for).and_return(0)
      allow(transaction_manager).to receive(:producer_id).and_return(-1)
      allow(transaction_manager).to receive(:producer_epoch).and_return(0)
      allow(transaction_manager).to receive(:transactional_id).and_return(nil)

      buffer.write(value: "Hello World", key: 1, topic: 'hello', partition: 0)
      buffer.write(value: "Bye World", key: 2, topic: 'hello', partition: 1)
      buffer.write(value: "Greeting", key: 3, topic: 'hi', partition: 0)
      buffer.write(value: "Farewell", key: 4, topic: 'hi', partition: 1)
      buffer.write(value: "Farewell 2", key: 5, topic: 'hi', partition: 1)
      buffer.write(value: "Good night", key: 6, topic: 'bye', partition: 0)
    end

    it 'sends produce requests to lead brokers of the partitions' do
      expect(broker_1).to receive(:produce).with(
        messages_for_topics: {
          'hello' => {
            0 => Kafka::Protocol::RecordBatch.new(
              records: Kafka::Protocol::Record.new(
                value: 'Hello World',
                key: 1
              )
            )
          }
        },
        compressor: compressor,
        required_acks: -1,
        timeout: 5000,
        transactional_id: nil
      )

      expect(broker_2).to receive(:produce).with(
        messages_for_topics: {
          'hello' => {
            1 => Kafka::Protocol::RecordBatch.new(
              records: [Kafka::Protocol::Record.new(
                value: 'Bye World',
                key: 2
              )]
            )
          }
        },
        compressor: compressor,
        required_acks: -1,
        timeout: 5000,
        transactional_id: nil
      )

      expect(broker_3).to receive(:produce).with(
        messages_for_topics: {
          'hi' => {
            0 => Kafka::Protocol::RecordBatch.new(
              records: Kafka::Protocol::Record.new(
                value: 'Greeting',
                key: 3
              )
            ),
            1 => Kafka::Protocol::RecordBatch.new(
              records: [
                Kafka::Protocol::Record.new(
                  value: 'Farewell',
                  key: 4
                ),
                Kafka::Protocol::Record.new(
                  value: 'Farewell',
                  key: 5
                )
              ]
            )
          },
          'bye' => {
            0 => Kafka::Protocol::RecordBatch.new(
              records: Kafka::Protocol::Record.new(
                value: 'Good night',
                key: 6
              )
            )
          }
        },
        compressor: compressor,
        required_acks: -1,
        timeout: 5000,
        transactional_id: nil
      )

      operation.execute
    end
  end

  context 'idempotent production' do
  end
end
