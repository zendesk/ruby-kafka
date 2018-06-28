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
      @logger = logger

      @transactional = transactional
      @transactional_id = transactional_id
      @transactional_timeout = transactional_timeout
      @transaction_state = Kafka::TransactionStateMachine.new(logger: logger)
      @transaction_partitions = {}

      # If transactional mode is enabled, idempotent must be enabled
      @idempotent = transactional ? true : idempotent

      @producer_id = -1
      @producer_epoch = 0

      @sequences = {}
    end

    def idempotent?
      @idempotent == true
    end

    def transactional?
      @idempotent == true
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

      nil
    end

    def add_partitions_to_transaction(topic_partitions)
      force_transactional!

      # Extract newly created partitions
      new_topic_partitions = {}
      topic_partitions.each do |topic, partitions|
        partitions.each do |partition|
          @transaction_partitions[topic] ||= {}
          if !@transaction_partitions[topic][partition]
            new_topic_partitions[topic] = []
            new_topic_partitions[topic] << partition
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
        response.errors do |tp|
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
      raise 'Transaction has already started' if @transaction_state.in_transaction?
      raise 'Transaction is not ready' unless @transaction_state.ready?
      @transaction_state.transition_to!(TransactionStateMachine::IN_TRANSACTION)

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
        raise 'Transaction is not valid to commit'
      end

      @transaction_state.transition_to!(TransactionStateMachine::COMMITTING_TRANSACTION)
      response = transaction_coordinator.end_txn(
        transactional_id: @transactional_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        transaction_result: TRANSACTION_RESULT_COMMIT
      )
      Protocol.handle_error(response.error_code)
      @transaction_state.transition_to!(TransactionStateMachine::READY)

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
        raise 'Transaction is not valid to abort'
      end

      @transaction_state.transition_to!(TransactionStateMachine::ABORTING_TRANSACTION)
      response = transaction_coordinator.end_txn(
        transactional_id: @transactional_id,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        transaction_result: TRANSACTION_RESULT_ABORT
      )
      Protocol.handle_error(response)
      @transaction_state.transition_to!(TransactionStateMachine::READY)

      nil
    rescue
      @transaction_state.transition_to!(TransactionStateMachine::ERROR)
      raise
    end

    def in_transaction?
      @transaction_state.in_transaction?
    end

    private

    def force_transactional!
      unless transactional?
        raise 'Please turn on transactional mode to use transaction'
      end

      if @transactional_id.nil? || @transactional_id.empty?
        raise 'Please provide a transaction_id to use transactional mode'
      end
    end

    def transaction_coordinator
      @cluster.get_transaction_coordinator(
        transactional_id: @transactional_id
      )
    end
  end
end
