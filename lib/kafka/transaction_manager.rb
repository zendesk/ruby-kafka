# frozen_string_literal: true

require 'kafka/transaction_state_machine'

module Kafka
  class TransactionManager
    DEFAULT_TRANSACTION_TIMEOUT = 60 # 60 seconds
    TRANSACTION_RESULT_COMMIT = true
    TRANSACTION_RESULT_ABORT = false

    attr_reader :producer_id, :producer_epoch, :transactional_id

    def initialize(
      cluster:,
      logger:,
      idempotent: false,
      transactional: false,
      transactional_id: nil,
      transactional_timeout: DEFAULT_TRANSACTION_TIMEOUT
    )
      @cluster = cluster
      @logger = TaggedLogger.new(logger)

      @transactional = transactional
      @transactional_id = transactional_id
      @transactional_timeout = transactional_timeout
      @transaction_state = Kafka::TransactionStateMachine.new(logger: logger)
      @transaction_partitions = {}

      # If transactional mode is enabled, idempotent must be enabled
      @idempotent = transactional || idempotent

      @producer_id = -1
      @producer_epoch = 0

      @sequences = {}
    end

    def idempotent?
      @idempotent == true
    end

    def transactional?
      @transactional == true && !@transactional_id.nil?
    end

    def init_producer_id(force = false)
      return if @producer_id >= 0 && !force

      response = transaction_coordinator.init_producer_id(
        transactional_id: @transactional_id,
        transactional_timeout: @transactional_timeout
      )
      Protocol.handle_error(response.error_code)

      # Reset producer id
      @producer_id = response.producer_id
      @producer_epoch = response.producer_epoch

      # Reset sequence
      @sequences = {}

      @logger.debug "Current Producer ID is #{@producer_id} and Producer Epoch is #{@producer_epoch}"
    end

    def next_sequence_for(topic, partition)
      @sequences[topic] ||= {}
      @sequences[topic][partition] ||= 0
    end

    def update_sequence_for(topic, partition, sequence)
      @sequences[topic] ||= {}
      @sequences[topic][partition] = sequence
    end

    def init_transactions
      force_transactional!
      unless @transaction_state.uninitialized?
        @logger.warn("Transaction already initialized!")
        return
      end
      init_producer_id(true)
      @transaction_partitions = {}
      @transaction_state.transition_to!(TransactionStateMachine::READY)

      @logger.info "Transaction #{@transactional_id} is initialized, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def add_partitions_to_transaction(topic_partitions)
      force_transactional!

      if @transaction_state.uninitialized?
        raise Kafka::InvalidTxnStateError, 'Transaction is uninitialized'
      end

      # Extract newly created partitions
      new_topic_partitions = {}
      topic_partitions.each do |topic, partitions|
        partitions.each do |partition|
          @transaction_partitions[topic] ||= {}
          if !@transaction_partitions[topic][partition]
            new_topic_partitions[topic] ||= []
            new_topic_partitions[topic] << partition

            @logger.info "Adding parition #{topic}/#{partition}  to transaction #{@transactional_id}, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"
          end
        end
      end

      unless new_topic_partitions.empty?
        response = transaction_coordinator.add_partitions_to_txn(
          transactional_id: @transactional_id,
          producer_id: @producer_id,
          producer_epoch: @producer_epoch,
          topics: new_topic_partitions
        )

        # Update added topic partitions
        response.errors.each do |tp|
          tp.partitions.each do |p|
            Protocol.handle_error(p.error_code)
            @transaction_partitions[tp.topic] ||= {}
            @transaction_partitions[tp.topic][p.partition] = true
          end
        end
      end

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def begin_transaction
      force_transactional!
      raise Kafka::InvalidTxnStateError, 'Transaction has already started' if @transaction_state.in_transaction?
      raise Kafka::InvalidTxnStateError, 'Transaction is not ready' unless @transaction_state.ready?
      @transaction_state.transition_to!(TransactionStateMachine::IN_TRANSACTION)

      @logger.info "Begin transaction #{@transactional_id}, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def commit_transaction
      force_transactional!

      if @transaction_state.committing_transaction?
        @logger.warn("Transaction is being committed")
        return
      end

      unless @transaction_state.in_transaction?
        raise Kafka::InvalidTxnStateError, 'Transaction is not valid to commit'
      end

      @transaction_state.transition_to!(TransactionStateMachine::COMMITTING_TRANSACTION)

      @logger.info "Commiting transaction #{@transactional_id}, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"

      response = transaction_coordinator.end_txn(
        transactional_id: @transactional_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        transaction_result: TRANSACTION_RESULT_COMMIT
      )
      Protocol.handle_error(response.error_code)

      @logger.info "Transaction #{@transactional_id} is committed, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"
      complete_transaction

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def abort_transaction
      force_transactional!

      if @transaction_state.aborting_transaction?
        @logger.warn("Transaction is being aborted")
        return
      end

      unless @transaction_state.in_transaction?
        @logger.warn('Aborting transaction that was never opened on brokers')
        return
      end

      @transaction_state.transition_to!(TransactionStateMachine::ABORTING_TRANSACTION)

      @logger.info "Aborting transaction #{@transactional_id}, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"

      response = transaction_coordinator.end_txn(
        transactional_id: @transactional_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        transaction_result: TRANSACTION_RESULT_ABORT
      )
      Protocol.handle_error(response.error_code)

      @logger.info "Transaction #{@transactional_id} is aborted, Producer ID: #{@producer_id} (Epoch #{@producer_epoch})"

      complete_transaction

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def send_offsets_to_txn(offsets:, group_id:)
      force_transactional!

      unless @transaction_state.in_transaction?
        raise Kafka::InvalidTxnStateError, 'Transaction is not valid to send offsets'
      end

      add_response = transaction_coordinator.add_offsets_to_txn(
        transactional_id: @transactional_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        group_id: group_id
      )
      Protocol.handle_error(add_response.error_code)

      send_response = group_coordinator(group_id: group_id).txn_offset_commit(
        transactional_id: @transactional_id,
        group_id: group_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        offsets: offsets
      )
      send_response.errors.each do |tp|
        tp.partitions.each do |partition|
          Protocol.handle_error(partition.error_code)
        end
      end

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def in_transaction?
      @transaction_state.in_transaction?
    end

    def error?
      @transaction_state.error?
    end

    def ready?
      @transaction_state.ready?
    end

    def close
      if in_transaction?
        @logger.warn("Aborting pending transaction ...")
        abort_transaction
      elsif @transaction_state.aborting_transaction? || @transaction_state.committing_transaction?
        @logger.warn("Transaction is finishing. Sleeping until finish!")
        sleep 5
      end
    end

    private

    def force_transactional!
      unless transactional?
        raise Kafka::InvalidTxnStateError, 'Please turn on transactional mode to use transaction'
      end

      if @transactional_id.nil? || @transactional_id.empty?
        raise Kafka::InvalidTxnStateError, 'Please provide a transaction_id to use transactional mode'
      end
    end

    def transaction_coordinator
      @cluster.get_transaction_coordinator(
        transactional_id: @transactional_id
      )
    end

    def group_coordinator(group_id:)
      @cluster.get_group_coordinator(
        group_id: group_id
      )
    end

    def complete_transaction
      @transaction_state.transition_to!(TransactionStateMachine::READY)
      @transaction_partitions = {}
    end
  end
end
