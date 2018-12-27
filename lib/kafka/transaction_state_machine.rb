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
      @logger = TaggedLogger.new(logger)
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
      in_state?(UNINITIALIZED)
    end

    def ready?
      in_state?(READY)
    end

    def in_transaction?
      in_state?(IN_TRANSACTION)
    end

    def committing_transaction?
      in_state?(COMMITTING_TRANSACTION)
    end

    def aborting_transaction?
      in_state?(ABORTING_TRANSACTION)
    end

    def error?
      in_state?(ERROR)
    end

    private

    def in_state?(state)
      @mutex.synchronize { @state == state }
    end
  end
end
