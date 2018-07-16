# frozen_string_literal: true

require 'timecop'

describe Kafka::Heartbeat do

  let(:group) { double(:group) }
  let(:consumer) { double(:consumer) }
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }

  let(:heartbeat) do
    described_class.new(group: group, consumer: consumer,
                        logger: logger, instrumenter: instrumenter,
                        interval: 10, poll_timeout: 30
    )
  end

  before do
    allow(group).to receive(:assigned_partitions).and_return('topic1' => [1, 2])
    allow(group).to receive(:member?).and_return(true)
    allow(group).to receive(:group_id).and_return("test")
    allow(consumer).to receive(:join_group)
    allow(Thread).to receive(:new) do |&block|
      block.call
    end
    stub_const("Kafka::Heartbeat::LOOP_WAIT_TIME", 0)
  end

  describe '#start' do
    it 'should only start once' do
      expect(heartbeat).to receive(:start_heartbeat_loop).once
      heartbeat.start
      heartbeat.start
    end
  end

  describe '#start_heartbeat_loop' do
    it 'should stop loop if given a stop command' do
      expect(heartbeat).to receive(:stop).and_call_original
      expect(heartbeat).to receive(:send_heartbeat_if_needed).once do
        heartbeat.stop
      end
      heartbeat.send(:start_heartbeat_loop)
    end
  end

  describe 'send_heartbeat_if_needed' do
    it "should do nothing if interval isn't reached yet" do
      heartbeat.reset_timestamps
      expect(heartbeat).not_to receive(:trigger!)
      heartbeat.send(:send_heartbeat_if_needed)
    end

    context 'after interval is reached' do
      before do
        Timecop.freeze(1)
        heartbeat.reset_timestamps
        Timecop.freeze(11)
      end

      it 'should trigger' do
        expect(heartbeat).to receive(:trigger!)
        heartbeat.send(:send_heartbeat_if_needed)
      end

      it 'should not trigger if the group is not a member' do
        allow(group).to receive(:member?).and_return(false)
        expect(heartbeat).not_to receive(:trigger!)
        heartbeat.send(:send_heartbeat_if_needed)
      end

      it 'should leave the group if poll timeout is reached' do
        Timecop.freeze(31)
        expect(group).to receive(:leave)
        expect(heartbeat).not_to receive(:trigger!)
        heartbeat.send(:send_heartbeat_if_needed)
      end

      it 'should rejoin the group if it gets an error' do
        expect(consumer).to receive(:join_group)
        allow(heartbeat).to receive(:trigger!).and_raise Kafka::HeartbeatError
        heartbeat.send(:send_heartbeat_if_needed)
      end
    end

  end
end
