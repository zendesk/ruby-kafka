# frozen_string_literal: true

describe Kafka::LockedQueue do
  context '1 producer 1 consumer' do
    let!(:queue) { described_class.new }

    it 'transfers the data to the producer' do
      records = []
      t = Thread.new do
        loop do
          records << queue.deq
          sleep 0.2
        end
      end
      t.abort_on_exception = true

      queue << 'one'
      queue << 'two'
      queue << 'three'
      sleep 1

      expect(records).to eql(%w(one two three))
      t.kill
    end
  end

  context '1 producer 3 consumers' do
    let!(:queue) { described_class.new }

    it 'spreads the data to consumers' do
      records = {}
      threads = (1..3).map do
        Thread.new do
          loop do
            records[Thread.current] ||= []
            records[Thread.current] << queue.deq
            sleep 1
          end
        end.tap do |thread|
          thread.abort_on_exception = true
        end
      end

      queue << 'one'
      queue << 'two'
      queue << 'three'
      queue << 'four'
      sleep 0.5
      expect(records.values.flatten).to match_array(%w(one two three))
      expect(records.keys.count).to eql(3)
      sleep 1
      expect(records.values.flatten).to match_array(%w(one two three four))
      expect(records.keys.count).to eql(3)
      threads.map(&:kill)
    end
  end

  context 'map data' do
    let!(:queue) { described_class.new }

    it 'spreads the data to consumers' do
      records = []
      threads = (1..3).map do
        Thread.new do
          loop do
            records << queue.deq
            sleep 1
          end
        end.tap do |thread|
          thread.abort_on_exception = true
        end
      end

      queue << 'one'
      queue << 'two'
      queue << 'three'
      queue << 'four'
      queue << 'five'
      sleep 0.5
      queue.map! do |value|
        value * 2
      end
      sleep 2
      expect(records).to match_array(
        %w(one two three fourfour fivefive)
      )
      threads.map(&:kill)
    end
  end
end
