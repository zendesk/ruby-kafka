# frozen_string_literal: true

context ::Kafka::TransactionStateMachine do
  let(:state_machine) { ::Kafka::TransactionStateMachine.new(logger: LOGGER) }

  context 'initialized state' do
    it 'sets state to Uninitialized' do
      expect(state_machine.uninitialized?).to eql(true)
    end
  end

  context '#transition_to!' do
    context 'transaction to ready' do
      context 'current state is uninitialized' do
        it 'sets state to Ready' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          expect(state_machine.ready?).to eql(true)
        end
      end

      context 'current state is ready' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is in_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is aborting_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
        end

        it 'sets state to ready' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          expect(state_machine.ready?).to eql(true)
        end
      end

      context 'current state is committing_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
        end

        it 'sets state to ready' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          expect(state_machine.ready?).to eql(true)
        end
      end
    end

    context 'transaction to in_transaction' do
      context 'current state is uninitialized' do
        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is ready' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
        end

        it 'sets state to in_transaction' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          expect(state_machine.in_transaction?).to eql(true)
        end
      end

      context 'current state is in_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is aborting_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is committing_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end
    end

    context 'transaction to aborting_transaction' do
      context 'current state is uninitialized' do
        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is ready' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is in_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
        end

        it 'sets state to aborting_transaction' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
          expect(state_machine.aborting_transaction?).to eql(true)
        end
      end

      context 'current state is aborting_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is committing_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end
    end

    context 'transaction to committing_transaction' do
      context 'current state is uninitialized' do
        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is ready' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is in_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
        end

        it 'sets state to committing_transaction' do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
          expect(state_machine.committing_transaction?).to eql(true)
        end
      end

      context 'current state is aborting_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::ABORTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end

      context 'current state is committing_transaction' do
        before do
          state_machine.transition_to!(::Kafka::TransactionStateMachine::READY)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::IN_TRANSACTION)
          state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
        end

        it 'raises exception' do
          expect do
            state_machine.transition_to!(::Kafka::TransactionStateMachine::COMMITTING_TRANSACTION)
          end.to raise_error(::Kafka::TransactionStateMachine::InvalidTransitionError)
        end
      end
    end

    context 'transaction to invalid state' do
      it 'raises exception' do
        expect do
          state_machine.transition_to!('Wrong state')
        end.to raise_error(::Kafka::TransactionStateMachine::InvalidStateError)
      end
    end
  end
end
