# frozen_string_literal: true

module Kafka
  class TransactionStateMachine
    class InvalidTransitionError < StandardError; end
    class InvalidStateError < StandardError; end

    STATES = [
      UNINITIALIZED          = :uninitialized,
      READY                  = :ready,
      IN_TRANSACTION         = :in_trasaction,
      COMMITTING_TRANSACTION = :committing_transaction,
      ABORTING_TRANSACTION   = :aborting_transaction,
      ERROR                  = :error
    ]

    TRANSITIONS = {
      UNINITIALIZED          => [READY, ERROR],
      READY                  => [UNINITIALIZED, COMMITTING_TRANSACTION, ABORTING_TRANSACTION],
      IN_TRANSACTION         => [READY],
      COMMITTING_TRANSACTION => [IN_TRANSACTION],
      ABORTING_TRANSACTION   => [IN_TRANSACTION],
      # Any states can transition to error state
      ERROR                  => STATES
    }

    def initialize(logger:)
      @state = UNINITIALIZED
      @mutex = Mutex.new
      @logger = logger
    end

    def transition_to!(next_state)
      raise InvalidStateError unless STATES.include?(next_state)
      unless TRANSITIONS[next_state].include?(@state)
        raise InvalidTransitionError, "Could not transition from state '#{@state}' to state '#{next_state}'"
      end
      @logger.debug("Transaction state changed to '#{next_state}'!")
      @mutex.synchronize { @state = next_state }
    end

    def uninitialized?
      @mutex.synchronize { @state == UNINITIALIZED }
    end

    def ready?
      @mutex.synchronize { @state == READY }
    end

    def in_transaction?
      @mutex.synchronize { @state == IN_TRANSACTION }
    end

    def committing_transaction?
      @mutex.synchronize { @state == COMMITTING_TRANSACTION }
    end

    def aborting_transaction?
      @mutex.synchronize { @state == ABORTING_TRANSACTION }
    end

    def error?
      @mutex.synchronize { @state == ERROR }
    end
  end
end
