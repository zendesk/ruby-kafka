# frozen_string_literal: true

require "timecop"

describe Kafka::PauseManager do
  let(:manager) { described_class.new }

  describe '#paused?' do
    context 'call with non-existing partition' do
      it 'returns false' do
        expect(manager.paused?('hello', 12)).to eql(false)
      end
    end

    context 'partition already paused' do
      before do
        manager.pause!('hello', 12)
      end

      it 'returns true' do
        expect(manager.paused?('hello', 12)).to eql(true)
      end
    end

    context 'partition resumed' do
      before do
        manager.pause!('hello', 12)
        manager.resume!('hello', 12)
      end

      it 'returns false' do
        expect(manager.paused?('hello', 12)).to eql(false)
      end
    end

    context 'partition pause expired' do
      it do
        time = Time.now

        Timecop.freeze time do
          manager.pause!('hello', 12, timeout: 5)
          Timecop.travel 3 do
            expect(manager.paused?('hello', 12)).to eql(true)
          end
          Timecop.travel 6 do
            expect(manager.paused?('hello', 12)).to eql(false)
          end
        end
      end
    end
  end
end
