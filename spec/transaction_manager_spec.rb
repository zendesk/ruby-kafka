require 'spec_helper'

describe ::Kafka::TransactionManager do
  let!(:logger) { LOGGER }
  let!(:cluster) { double(:cluster) }
  let!(:transaction_coordinator) { double(:broker) }
  let!(:group_coordinator) { double(:broker) }

  let!(:manager) do
    described_class.new(logger: logger, cluster: cluster)
  end

  before do
    allow(cluster).to receive(:get_transaction_coordinator).and_return(
      transaction_coordinator
    )
    allow(cluster).to receive(:get_group_coordinator).and_return(
      group_coordinator
    )
    allow(transaction_coordinator).to receive(:init_producer_id).and_return(
      Kafka::Protocol::InitProducerIDResponse.new(
        error_code: 0,
        producer_id: 1234,
        producer_epoch: 1
      )
    )
  end

  describe '#init_producer_id' do
    let!(:manager) do
      described_class.new(logger: logger, cluster: cluster, transactional_id: 'IDID', transactional_timeout: 600)
    end

    context 'producer_id not exist' do
      it 'fetches producer information from transaction coordinator' do
        expect(transaction_coordinator).to receive(:init_producer_id).with(
          transactional_id: 'IDID',
          transactional_timeout: 600
        )
        manager.init_producer_id
        expect(manager.producer_id).to eql(1234)
        expect(manager.producer_epoch).to eql(1)
      end
    end

    context 'producer_id already exists' do
      before do
        manager.init_producer_id

        allow(transaction_coordinator).to receive(:init_producer_id).and_return(
          Kafka::Protocol::InitProducerIDResponse.new(
            error_code: 0,
            producer_id: 456,
            producer_epoch: 2
          )
        )
      end

      context 'without force option' do
        it 'does not updates producer info' do
          expect(transaction_coordinator).not_to receive(:init_producer_id)
          manager.init_producer_id
          expect(manager.producer_id).to eql(1234)
          expect(manager.producer_epoch).to eql(1)
        end
      end

      context 'with force option' do
        it 'reloads the producer information' do
          expect(transaction_coordinator).to receive(:init_producer_id).with(
            transactional_id: 'IDID',
            transactional_timeout: 600
          )
          manager.init_producer_id(true)
          expect(manager.producer_id).to eql(456)
          expect(manager.producer_epoch).to eql(2)
        end
      end
    end
  end

  describe '#next_sequence_for' do
    context 'topic not exist' do
      it 'returns 0' do
        expect(manager.next_sequence_for('hello', 10)).to eql(0)
      end
    end

    context 'partition not exist' do
      before do
        manager.update_sequence_for('hello', 0, 5)
      end

      it 'returns 0' do
        expect(manager.next_sequence_for('hello', 10)).to eql(0)
      end
    end

    context 'topic and partition exist' do
      before do
        manager.update_sequence_for('hello', 10, 8)
      end

      it 'returns current sequence' do
        expect(manager.next_sequence_for('hello', 10)).to eql(8)
      end
    end
  end

  describe '#add_partitions_to_transaction' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: '1234'
      )
    end

    context 'no partitions in the transaction yet' do
      before do
        manager.init_transactions
        allow(transaction_coordinator).to receive(:add_partitions_to_txn).and_return(
          success_add_partitions_to_txn_response(
            'hello' => [1],
            'world' => [2]
          )
        )
      end

      it 'calls the partition coordinator to add all partitions' do
        expect(transaction_coordinator).to receive(:add_partitions_to_txn).with(
          transactional_id: '1234',
          producer_id: 1234,
          producer_epoch: 1,
          topics: {
            'hello' => [1],
            'world' => [2]
          }
        )
        manager.add_partitions_to_transaction(
          'hello' => [1],
          'world' => [2]
        )
      end
    end

    context 'some partitions are in the transaction' do
      before do
        manager.init_transactions
        allow(transaction_coordinator).to receive(:add_partitions_to_txn).and_return(
          success_add_partitions_to_txn_response(
            'hello' => [1],
            'world' => [2]
          )
        )
        manager.add_partitions_to_transaction(
          'hello' => [1],
          'world' => [2]
        )
        allow(transaction_coordinator).to receive(:add_partitions_to_txn).and_return(
          success_add_partitions_to_txn_response(
            'hello' => [3],
            'bye' => [4]
          )
        )
      end

      it 'calls the partition coordinator to add new partitions' do
        expect(transaction_coordinator).to receive(:add_partitions_to_txn).with(
          transactional_id: '1234',
          producer_id: 1234,
          producer_epoch: 1,
          topics: {
            'hello' => [3],
            'bye' => [4]
          }
        )
        manager.add_partitions_to_transaction(
          'hello' => [1, 3],
          'world' => [2],
          'bye' => [4]
        )
      end
    end

    context 'there are not new partitions in the transaction' do
      before do
        manager.init_transactions
        allow(transaction_coordinator).to receive(:add_partitions_to_txn).and_return(
          success_add_partitions_to_txn_response(
            'hello' => [1, 2],
            'world' => [2]
          )
        )
        manager.add_partitions_to_transaction(
          'hello' => [1, 2],
          'world' => [2]
        )
      end

      it 'does not calls the partition coordinator' do
        expect(transaction_coordinator).not_to receive(:add_partitions_to_txn)
        manager.add_partitions_to_transaction(
          'hello' => [1],
          'world' => [2]
        )
      end
    end

    context 'manager is not transactional' do
      let!(:manager) { described_class.new(logger: logger, cluster: cluster) }

      it 'raises exception' do
        expect do
          manager.add_partitions_to_transaction(
            'hello' => [1, 2, 3]
          )
        end.to raise_error(Kafka::InvalidTxnStateError, /please turn on transactional mode/i)
      end
    end
  end

  describe '#init_transactions' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: 'IDID', transactional_timeout: 600
      )
    end

    it 'init producer information' do
      manager.init_transactions
      expect(manager.producer_id).to eql(1234)
      expect(manager.producer_epoch).to eql(1)
    end

    context 'current status is not initialize' do
      before do
        manager.init_transactions
        allow(transaction_coordinator).to receive(:init_producer_id).and_return(
          Kafka::Protocol::InitProducerIDResponse.new(
            error_code: 0,
            producer_id: 456,
            producer_epoch: 2
          )
        )
      end

      it 'does nothing' do
        manager.init_transactions
        expect(manager.producer_id).to eql(1234)
        expect(manager.producer_epoch).to eql(1)
      end
    end

    context 'manager is not in transactional mode' do
      let!(:manager) do
        described_class.new(logger: logger, cluster: cluster)
      end

      it 'raises exception' do
        expect do
          manager.init_transactions
        end.to raise_error(Kafka::InvalidTxnStateError, /please turn on transactional mode/i)
      end

      it 'changes state to error' do
        begin
          manager.init_transactions
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end
  end

  describe '#begin_transaction' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: 'IDID', transactional_timeout: 600
      )
    end

    it 'changes state to init_transaction' do
      manager.init_transactions
      manager.begin_transaction
      expect(manager.in_transaction?).to eql(true)
    end

    context 'transaction already started' do
      before do
        manager.init_transactions
        manager.begin_transaction
      end

      it 'raises exception' do
        expect do
          manager.begin_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /transaction has already started/i)
      end

      it 'changes state to error' do
        begin
          manager.begin_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'transaction is not initialized' do
      it 'raises exception' do
        expect do
          manager.begin_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /transaction is not ready/i)
      end

      it 'changes state to error' do
        begin
          manager.begin_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'manager is not in transactional mode' do
      let!(:manager) do
        described_class.new(logger: logger, cluster: cluster)
      end

      it 'raises exception' do
        expect do
          manager.begin_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /please turn on transactional mode/i)
      end

      it 'changes state to error' do
        begin
          manager.begin_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end
  end

  describe '#abort_transaction' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: 'IDID', transactional_timeout: 600
      )
    end

    before do
      allow(transaction_coordinator).to receive(:end_txn).and_return(
        Kafka::Protocol::EndTxnResposne.new(
          error_code: 0
        )
      )
    end

    context 'transaction is ready to abort' do
      before do
        manager.init_transactions
        manager.begin_transaction
      end

      it 'calls transaction coordinator to end TXN' do
        expect(transaction_coordinator).to receive(:end_txn).with(
          transactional_id: 'IDID',
          producer_id: 1234,
          producer_epoch: 1,
          transaction_result: false
        )
        manager.abort_transaction
        expect(manager.in_transaction?).to eql(false)
      end
    end

    context 'coordinator returns error' do
      before do
        manager.init_transactions
        manager.begin_transaction
        allow(transaction_coordinator).to receive(:end_txn).and_return(
          Kafka::Protocol::EndTxnResposne.new(
            error_code: 47
          )
        )
      end

      it 'raises exception' do
        expect do
          manager.abort_transaction
        end.to raise_error(Kafka::InvalidProducerEpochError)
      end

      it 'changes state to error' do
        begin
          manager.abort_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'manager is not in a transaction' do
      before do
        manager.init_transactions
      end

      it 'does not raise an exception' do
        expect { manager.abort_transaction }.not_to raise_error
      end

      it 'leaves transaction manager in ready state' do
        begin
          manager.abort_transaction
        rescue; end
        expect(manager.ready?).to eql(true)
      end
    end

    context 'manager is not in transactional mode' do
      let!(:manager) do
        described_class.new(logger: logger, cluster: cluster)
      end

      it 'raises exception' do
        expect do
          manager.abort_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /please turn on transactional mode/i)
      end

      it 'changes state to error' do
        begin
          manager.abort_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end
  end

  describe '#commit_transaction' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: 'IDID', transactional_timeout: 600
      )
    end

    before do
      allow(transaction_coordinator).to receive(:end_txn).and_return(
        Kafka::Protocol::EndTxnResposne.new(
          error_code: 0
        )
      )
    end

    context 'transaction is ready to commit' do
      before do
        manager.init_transactions
        manager.begin_transaction
      end

      it 'calls transaction coordinator to end TXN' do
        expect(transaction_coordinator).to receive(:end_txn).with(
          transactional_id: 'IDID',
          producer_id: 1234,
          producer_epoch: 1,
          transaction_result: true
        )
        manager.commit_transaction
        expect(manager.in_transaction?).to eql(false)
      end
    end

    context 'coordinator returns error' do
      before do
        manager.init_transactions
        manager.begin_transaction
        allow(transaction_coordinator).to receive(:end_txn).and_return(
          Kafka::Protocol::EndTxnResposne.new(
            error_code: 47
          )
        )
      end

      it 'raises exception' do
        expect do
          manager.commit_transaction
        end.to raise_error(Kafka::InvalidProducerEpochError)
      end

      it 'changes state to error' do
        begin
          manager.commit_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'manager is not in a transaction' do
      before do
        manager.init_transactions
      end

      it 'raises exception' do
        expect do
          manager.commit_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /transaction is not valid to commit/i)
      end

      it 'changes state to error' do
        begin
          manager.commit_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'manager is not in transactional mode' do
      let!(:manager) do
        described_class.new(logger: logger, cluster: cluster)
      end

      it 'raises exception' do
        expect do
          manager.commit_transaction
        end.to raise_error(Kafka::InvalidTxnStateError, /please turn on transactional mode/i)
      end

      it 'changes state to error' do
        begin
          manager.commit_transaction
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end
  end

  describe '#close' do
    let!(:manager) do
      described_class.new(
        logger: logger, cluster: cluster,
        transactional: true, transactional_id: 'IDID', transactional_timeout: 600
      )
    end

    before do
      allow(transaction_coordinator).to receive(:end_txn).and_return(
        Kafka::Protocol::EndTxnResposne.new(
          error_code: 0
        )
      )
    end

    context 'currently in a transaction' do
      before do
        manager.init_transactions
        manager.begin_transaction
      end

      it 'aborts transaction' do
        expect(transaction_coordinator).to receive(:end_txn).with(
          transactional_id: 'IDID',
          producer_id: 1234,
          producer_epoch: 1,
          transaction_result: false
        )
        manager.close
        expect(manager.in_transaction?).to eql(false)
      end
    end
  end

  describe '#send_offsets_to_txn' do
    let!(:manager) do
      described_class.new(
        logger: logger,
        cluster: cluster,
        transactional: true,
        transactional_id: 'IDID',
        transactional_timeout: 600
      )
    end

    context 'currently in transaction' do
      before do
        manager.init_transactions
        manager.begin_transaction
        allow(transaction_coordinator).to receive(:add_offsets_to_txn).and_return(
          Kafka::Protocol::AddOffsetsToTxnResponse.new(
            error_code: 0
          )
        )
        allow(group_coordinator).to receive(:txn_offset_commit).and_return(
          txn_offset_commit_response({
            'hello' => [1],
            'world' => [2]
          })
        )
      end

      it 'notifies transaction coordinator' do
        manager.send_offsets_to_txn(offsets: [1, 2], group_id: 1)
        expect(transaction_coordinator).to have_received(:add_offsets_to_txn)
        expect(group_coordinator).to have_received(:txn_offset_commit)
      end
    end

    context 'transaction coordinator returns error' do
      before do
        manager.init_transactions
        manager.begin_transaction
        allow(transaction_coordinator).to receive(:add_offsets_to_txn).and_return(
          Kafka::Protocol::AddOffsetsToTxnResponse.new(
            error_code: 47
          )
        )
      end

      it 'raises exception' do
        expect do
          manager.send_offsets_to_txn(offsets: [1, 2], group_id: 1)
        end.to raise_error(Kafka::InvalidProducerEpochError)
      end

      it 'changes state to error' do
        begin
          manager.send_offsets_to_txn(offsets: [1, 2], group_id: 1)
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end

    context 'group coordinator returns error' do
      before do
        manager.init_transactions
        manager.begin_transaction
        allow(transaction_coordinator).to receive(:add_offsets_to_txn).and_return(
          Kafka::Protocol::AddOffsetsToTxnResponse.new(
            error_code: 0
          )
        )
        allow(group_coordinator).to receive(:txn_offset_commit).and_return(
          txn_offset_commit_response(
            { 'hello' => [1], 'world' => [2] },
            error_code: 47
          )
        )
      end

      it 'raises exception' do
        expect do
          manager.send_offsets_to_txn(offsets: [1, 2], group_id: 1)
        end.to raise_error(Kafka::InvalidProducerEpochError)
      end

      it 'changes state to error' do
        begin
          manager.send_offsets_to_txn(offsets: [1, 2], group_id: 1)
        rescue; end
        expect(manager.error?).to eql(true)
      end
    end
  end
end

def success_add_partitions_to_txn_response(topics)
  Kafka::Protocol::AddPartitionsToTxnResponse.new(
    errors: topics.map do |topic, partitions|
      Kafka::Protocol::AddPartitionsToTxnResponse::TopicPartitionsError.new(
        topic: topic,
        partitions: partitions.map do |partition|
          Kafka::Protocol::AddPartitionsToTxnResponse::PartitionError.new(
            partition: partition,
            error_code: 0
          )
        end
      )
    end
  )
end

def txn_offset_commit_response(topics, error_code: 0)
  Kafka::Protocol::TxnOffsetCommitResponse.new(
    errors: topics.map do |topic, partitions|
      Kafka::Protocol::TxnOffsetCommitResponse::TopicPartitionsError.new(
        topic: topic,
        partitions: partitions.map do |partition|
          Kafka::Protocol::TxnOffsetCommitResponse::PartitionError.new(
            partition: partition,
            error_code: error_code
          )
        end
      )
    end
  )
end
