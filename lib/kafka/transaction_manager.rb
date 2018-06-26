# frozen_string_literal: true

module Kafka
  class TransactionManager
    DEFAULT_TRANSACTION_TIMEOUT = 60 # 60 seconds

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

      broker = @cluster.get_transaction_coordinator(
        transactional_id: @transactional_id
      )
      response = broker.init_producer_id(
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
  end
end
