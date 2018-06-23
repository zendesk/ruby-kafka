# frozen_string_literal: true

module Kafka
  class TransactionManager
    DEFAULT_TRANSACTION_TIMEOUT = 60 # 60 seconds

    attr_reader :producer_id, :producer_epoch, :transactional_id

    def initialize(
      cluster:,
      logger:,
      idempotence: false,
      transactional: false,
      transactional_id: nil,
      transactional_timeout: DEFAULT_TRANSACTION_TIMEOUT
    )
      @cluster = cluster
      @logger = logger

      @transactional = transactional
      @transactional_id = transactional_id
      @transactional_timeout = transactional_timeout

      # If transactional mode is enabled, idempotence must be enabled
      @idempotence = transactional ? true : idempotence

      @producer_id = -1
      @producer_epoch = 0
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

      @producer_id = response.producer_id
      @producer_epoch = response.producer_epoch
    end
  end
end
