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

  let(:next_sequences) {
    {
      'hello' => {
        0 => 99,
        1 => 100
      },
      'hi' => {
        0 => 0,
        1 => 1
      },
      'bye' => {
        0 => 50
      }
    }
  }

  before do
    allow(cluster).to receive(:get_leader).with('hello', 0).and_return(broker_1)
    allow(cluster).to receive(:get_leader).with('hello', 1).and_return(broker_2)
    allow(cluster).to receive(:get_leader).with('hi', 0).and_return(broker_3)
    allow(cluster).to receive(:get_leader).with('hi', 1).and_return(broker_3)
    allow(cluster).to receive(:get_leader).with('bye', 0).and_return(broker_3)

    allow(broker_1).to receive(:produce) do |request|
      generate_successful_produce_response(request)
    end

    allow(broker_2).to receive(:produce) do |request|
      generate_successful_produce_response(request)
    end

    allow(broker_3).to receive(:produce) do |request|
      generate_successful_produce_response(request)
    end
  end

  context 'normal message production' do
    before do
      allow(transaction_manager).to receive(:idempotent?).and_return(false)
      allow(transaction_manager).to receive(:transactional?).and_return(false)
      allow(transaction_manager).to receive(:next_sequence_for).and_return(0)
      allow(transaction_manager).to receive(:producer_id).and_return(-1)
      allow(transaction_manager).to receive(:producer_epoch).and_return(0)
      allow(transaction_manager).to receive(:transactional_id).and_return(nil)

      add_test_messages_to_buffer
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

  context 'idempotent message production' do
    before do
      allow(transaction_manager).to receive(:idempotent?).and_return(true)
      allow(transaction_manager).to receive(:transactional?).and_return(false)
      allow(transaction_manager).to receive(:producer_id).and_return(1234)
      allow(transaction_manager).to receive(:producer_epoch).and_return(12)
      allow(transaction_manager).to receive(:transactional_id).and_return(nil)
      allow(transaction_manager).to receive(:init_producer_id)

      allow(transaction_manager).to receive(:next_sequence_for) do |topic, partition|
        next_sequences[topic][partition]
      end
      add_test_messages_to_buffer
    end

    it 'sends produce requests to lead brokers of the partitions' do
      expect(broker_1).to receive(:produce).with(
        messages_for_topics: {
          'hello' => {
            0 => Kafka::Protocol::RecordBatch.new(
              first_sequence: 99,
              producer_id: 1234,
              producer_epoch: 12,
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
              first_sequence: 100,
              producer_id: 1234,
              producer_epoch: 12,
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
              first_sequence: 0,
              producer_id: 1234,
              producer_epoch: 12,
              records: Kafka::Protocol::Record.new(
                value: 'Greeting',
                key: 3
              )
            ),
            1 => Kafka::Protocol::RecordBatch.new(
              first_sequence: 1,
              producer_id: 1234,
              producer_epoch: 12,
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
              first_sequence: 50,
              producer_id: 1234,
              producer_epoch: 12,
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

      expect(transaction_manager).to receive(:init_producer_id).once
      expect(transaction_manager).to receive(:update_sequence_for).with('hello', 0, 100).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hello', 1, 101).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hi', 0, 1).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hi', 1, 3).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('bye', 0, 51).ordered
      operation.execute
    end

    context 'required_acks is different from -1' do
      let(:operation) {
        Kafka::ProduceOperation.new(
          cluster: cluster,
          transaction_manager: transaction_manager,
          buffer: buffer,
          compressor: compressor,
          logger: logger,
          instrumenter: instrumenter,
          required_acks: 2,
          ack_timeout: 5
        )
      }

      it 'raises exception' do
        expect do
          operation.execute
        end.to raise_error(/you must set required_acks option to :all to use idempotent/i)
      end
    end
  end

  context 'transactional message production' do
    before do
      allow(transaction_manager).to receive(:idempotent?).and_return(true)
      allow(transaction_manager).to receive(:transactional?).and_return(true)
      allow(transaction_manager).to receive(:producer_id).and_return(1234)
      allow(transaction_manager).to receive(:producer_epoch).and_return(12)
      allow(transaction_manager).to receive(:transactional_id).and_return('IDID')
      allow(transaction_manager).to receive(:init_producer_id)

      allow(transaction_manager).to receive(:next_sequence_for) do |topic, partition|
        next_sequences[topic][partition]
      end

      allow(transaction_manager).to receive(:in_transaction?).and_return(true)

      add_test_messages_to_buffer
    end

    it 'sends produce requests to lead brokers of the partitions' do
      expect(broker_1).to receive(:produce).with(
        messages_for_topics: {
          'hello' => {
            0 => Kafka::Protocol::RecordBatch.new(
              in_transaction: true,
              first_sequence: 99,
              producer_id: 1234,
              producer_epoch: 12,
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
        transactional_id: 'IDID'
      )

      expect(broker_2).to receive(:produce).with(
        messages_for_topics: {
          'hello' => {
            1 => Kafka::Protocol::RecordBatch.new(
              in_transaction: true,
              first_sequence: 100,
              producer_id: 1234,
              producer_epoch: 12,
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
        transactional_id: 'IDID'
      )

      expect(broker_3).to receive(:produce).with(
        messages_for_topics: {
          'hi' => {
            0 => Kafka::Protocol::RecordBatch.new(
              in_transaction: true,
              first_sequence: 0,
              producer_id: 1234,
              producer_epoch: 12,
              records: Kafka::Protocol::Record.new(
                value: 'Greeting',
                key: 3
              )
            ),
            1 => Kafka::Protocol::RecordBatch.new(
              in_transaction: true,
              first_sequence: 1,
              producer_id: 1234,
              producer_epoch: 12,
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
              in_transaction: true,
              first_sequence: 50,
              producer_id: 1234,
              producer_epoch: 12,
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
        transactional_id: 'IDID'
      )

      expect(transaction_manager).to receive(:init_producer_id).once
      expect(transaction_manager).to receive(:add_partitions_to_transaction).with(
        'hello' => [0, 1],
        'hi' => [0, 1],
        'bye' => [0]
      ).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hello', 0, 100).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hello', 1, 101).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hi', 0, 1).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('hi', 1, 3).ordered
      expect(transaction_manager).to receive(:update_sequence_for).with('bye', 0, 51).ordered
      operation.execute
    end

    context 'transaction is not pending' do
      before do
        allow(transaction_manager).to receive(:in_transaction?).and_return(false)
      end

      it 'raises exception' do
        expect do
          operation.execute
        end.to raise_error(/can only be executed in a pending transaction/i)
      end
    end

    context 'required_acks is different from -1' do
      let(:operation) {
        Kafka::ProduceOperation.new(
          cluster: cluster,
          transaction_manager: transaction_manager,
          buffer: buffer,
          compressor: compressor,
          logger: logger,
          instrumenter: instrumenter,
          required_acks: 2,
          ack_timeout: 5
        )
      }

      it 'raises exception' do
        expect do
          operation.execute
        end.to raise_error(/you must set required_acks option to :all to use idempotent/i)
      end
    end
  end
end

def add_test_messages_to_buffer
  buffer.write(value: "Hello World", key: 1, topic: 'hello', partition: 0)
  buffer.write(value: "Bye World", key: 2, topic: 'hello', partition: 1)
  buffer.write(value: "Greeting", key: 3, topic: 'hi', partition: 0)
  buffer.write(value: "Farewell", key: 4, topic: 'hi', partition: 1)
  buffer.write(value: "Farewell 2", key: 5, topic: 'hi', partition: 1)
  buffer.write(value: "Good night", key: 6, topic: 'bye', partition: 0)
end

def generate_successful_produce_response(request)
  Kafka::Protocol::ProduceResponse.new(
    topics: request[:messages_for_topics].map do |topic, topic_request|
      Kafka::Protocol::ProduceResponse::TopicInfo.new(
        topic: topic,
        partitions: topic_request.keys.map do |partition|
          Kafka::Protocol::ProduceResponse::PartitionInfo.new(
            partition: partition,
            error_code: 0,
            offset: 0,
            timestamp: Time.now
          )
        end
      )
    end
  )
end
