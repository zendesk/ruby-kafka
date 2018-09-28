# frozen_string_literal: true

require "timecop"

describe Kafka::Consumer do
  let(:cluster) { double(:cluster) }
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }
  let(:group) { double(:group) }
  let(:offset_manager) { double(:offset_manager) }
  let(:heartbeat) { double(:heartbeat) }
  let(:pause_manager) { Kafka::PauseManager.new }
  let(:fetcher) { double(:fetcher, configure: nil, subscribe: nil, seek: nil, start: nil, stop: nil) }
  let(:session_timeout) { 30 }
  let(:assigned_partitions) { { "greetings" => [0] } }

  let(:consumer) {
    Kafka::Consumer.new(
      cluster: cluster,
      logger: logger,
      instrumenter: instrumenter,
      group: group,
      offset_manager: offset_manager,
      fetcher: fetcher,
      session_timeout: session_timeout,
      heartbeat: heartbeat,
      pause_manager: pause_manager,
    )
  }

  let(:messages) {
    [
      double(:message, {
        value: "hello",
        key: nil,
        headers: {},
        topic: "greetings",
        partition: 0,
        offset: 13,
        create_time: Time.now,
      })
    ]
  }

  let(:fetched_batches) {
    [
      Kafka::FetchedBatch.new(
        topic: "greetings",
        partition: 0,
        last_offset: 13,
        highwater_mark_offset: 42,
        messages: messages,
      )
    ]
  }

  shared_context 'from unassigned partition' do
    let(:unassigned_messages) do
      [
        double(:message, {
          value: "hello",
          key: nil,
          topic: "greetings",
          partition: 1,
          offset: 10,
          create_time: Time.now,
        })
      ]
    end

    let(:old_fetched_batches) {
      [
        Kafka::FetchedBatch.new(
          topic: "greetings",
          partition: 1,
          last_offset: 10,
          highwater_mark_offset: 42,
          messages: unassigned_messages,
        )
      ]
    }

    before do
      @count = 0
      allow(fetcher).to receive(:poll) {
        @count += 1
        if @count == 1
          [:batches, old_fetched_batches]
        else
          [:batches, fetched_batches]
        end
      }
    end
  end

  shared_context 'with partition reassignment' do
    let(:messages_after_partition_reassignment) {
      [
        double(:message, {
          value: "hello",
          key: nil,
          headers: {},
          topic: "greetings",
          partition: 1,
          offset: 10,
          create_time: Time.now,
          is_control_record: false
        })
      ]
    }

    let(:batches_after_partition_reassignment) {
      [
        Kafka::FetchedBatch.new(
          topic: "greetings",
          partition: 1,
          last_offset: 10,
          highwater_mark_offset: 42,
          messages: messages_after_partition_reassignment,
        )
      ]
    }

    before do
      @count = 0
      allow(fetcher).to receive(:poll) {
        @count += 1
        if @count == 1
          [:batches, fetched_batches]
        else
          [:batches, batches_after_partition_reassignment]
        end
      }
    end
  end

  before do
    allow(cluster).to receive(:add_target_topics)
    allow(cluster).to receive(:disconnect)
    allow(cluster).to receive(:refresh_metadata_if_necessary!)

    allow(offset_manager).to receive(:commit_offsets)
    allow(offset_manager).to receive(:commit_offsets_if_necessary)
    allow(offset_manager).to receive(:set_default_offset)
    allow(offset_manager).to receive(:mark_as_processed)
    allow(offset_manager).to receive(:next_offset_for) { 42 }

    allow(group).to receive(:subscribe)
    allow(group).to receive(:group_id)
    allow(group).to receive(:leave)
    allow(group).to receive(:member?) { true }
    allow(group).to receive(:subscribed_partitions) { assigned_partitions }
    allow(group).to receive(:assigned_to?) { false }
    allow(group).to receive(:assigned_to?).with('greetings', 0) { true }

    allow(heartbeat).to receive(:trigger)

    allow(fetcher).to receive(:data?) { fetched_batches.any? }
    allow(fetcher).to receive(:poll) { [:batches, fetched_batches] }

    consumer.subscribe("greetings")
  end

  describe "#each_message" do
    let(:messages) {
      [
        double(:message, {
          value: "hello",
          key: nil,
          headers: {},
          topic: "greetings",
          partition: 0,
          offset: 13,
          create_time: Time.now,
          is_control_record: false
        })
      ]
    }

    let(:fetched_batches) {
      [
        Kafka::FetchedBatch.new(
          topic: "greetings",
          partition: 0,
          last_offset: 13,
          highwater_mark_offset: 42,
          messages: messages,
        )
      ]
    }

    it "instruments" do
      expect(instrumenter).to receive(:instrument).once.with('start_process_message.consumer', anything)
      expect(instrumenter).to receive(:instrument).once.with('process_message.consumer', anything)

      allow(instrumenter).to receive(:instrument).and_call_original

      consumer.each_message do |message|
        consumer.stop
      end
    end

    it "raises ProcessingError if the processing code fails" do
      expect {
        consumer.each_message do |message|
          raise "yolo"
        end
      }.to raise_exception(Kafka::ProcessingError) {|exception|
        expect(exception.topic).to eq "greetings"
        expect(exception.partition).to eq 0
        expect(exception.offset).to eq 13

        cause = exception.cause

        expect(cause.class).to eq RuntimeError
        expect(cause.message).to eq "yolo"
      }

      expect(log.string).to include "Exception raised when processing greetings/0 at offset 13 -- RuntimeError: yolo"
    end

    it "stops if SignalException is encountered" do
      allow(fetcher).to receive(:poll) { [:exception, SignalException.new("SIGTERM")] }

      consumer.each_message {}

      expect(log.string).to include "Received signal SIGTERM, shutting down"
    end

    it "seeks to the default offset when the checkpoint is invalid " do
      done = false

      allow(offset_manager).to receive(:seek_to_default) { done = true }

      allow(fetcher).to receive(:poll) {
        if done
          [:batches, fetched_batches]
        else
          [:exception, Kafka::OffsetOutOfRange.new]
        end
      }

      consumer.each_message do |message|
        consumer.stop
      end

      expect(offset_manager).to have_received(:seek_to_default)
    end

    it "does not fetch messages from paused partitions" do
      allow(group).to receive(:assigned_to?).with('greetings', 42) { true }

      assigned_partitions["greetings"] << 42

      consumer.pause("greetings", 42)

      consumer.each_message do |message|
        consumer.stop
      end

      expect(fetcher).to_not have_received(:seek).with("greetings", 42, anything)

      consumer.resume("greetings", 42)

      consumer.each_message do |message|
        consumer.stop
      end

      expect(fetcher).to have_received(:seek).with("greetings", 42, anything)
    end

    it "does not seek (previously) paused partition when not in group" do
      allow(group).to receive(:assigned_to?).with('greetings', 42) { false }

      assigned_partitions["greetings"] << 42

      consumer.pause("greetings", 42)
      consumer.resume("greetings", 42)

      consumer.each_message do |message|
        consumer.stop
      end

      expect(fetcher).to_not have_received(:seek).with("greetings", 42, anything)
    end

    it "automatically resumes partitions if a timeout is set" do
      time = Time.now

      Timecop.freeze time do
        consumer.pause("greetings", 0, timeout: 30)

        Timecop.travel 29 do
          expect(consumer.paused?("greetings", 0)).to eq true
        end

        Timecop.travel 31 do
          expect(consumer.paused?("greetings", 0)).to eq false
        end
      end
    end

    it 'retries final offsets commit at the end' do
      allow(offset_manager).to receive(:commit_offsets)
        .exactly(4).times { raise(Kafka::ConnectionError) }

      consumer.each_message do |message|
        consumer.stop
      end
    end

    context 'message from #fetch_batches is old, and from a partition not assigned to this consumer' do
      include_context 'from unassigned partition'

      it 'does not update offsets for messages from unassigned partitions' do
        consumer.each_message do |message|
          consumer.stop
        end

        expect(offset_manager).to have_received(:commit_offsets_if_necessary).twice

        offsets = consumer.instance_variable_get(:@current_offsets)
        expect(offsets['greetings'].keys).not_to include(1)
      end

      it 'does not process messages from unassigned partitions' do
        @yield_count = 0

        consumer.each_message do |message|
          @yield_count += 1
          consumer.stop
        end

        expect(offset_manager).to have_received(:commit_offsets_if_necessary).twice
        expect(@yield_count).to eq 1
      end
    end

    context 'consumer joins a new group' do
      include_context 'with partition reassignment'

      let(:group) { double(:group).as_null_object }
      let(:fetcher) { double(:fetcher).as_null_object }
      let(:current_offsets) { consumer.instance_variable_get(:@current_offsets) }
      let(:assigned_partitions) { { 'greetings' => [0] } }
      let(:reassigned_partitions) { { 'greetings' => [1] } }

      before do
        allow(heartbeat).to receive(:trigger) do
          next unless @encounter_rebalance
          @encounter_rebalance = false
          raise Kafka::RebalanceInProgress
        end

        consumer.each_message do |message|
          consumer.stop
        end

        allow(group).to receive(:assigned_partitions).and_return(reassigned_partitions)
        allow(group).to receive(:assigned_to?).with('greetings', 1) { true }
        allow(group).to receive(:assigned_to?).with('greetings', 0) { false }
        allow(group).to receive(:generation_id).and_return(*generation_ids)

        @encounter_rebalance = true
      end

      context 'with subsequent group generations' do
        let(:generation_ids) { [1, 2] }

        it 'removes local offsets for partitions it is no longer assigned' do
          expect(offset_manager).to receive(:clear_offsets_excluding).with(reassigned_partitions)

          expect do
            consumer.each_message do |message|
              consumer.stop
            end
          end.to change { current_offsets['greetings'].keys }.from([0]).to([1])
        end
      end

      context 'with group generations further apart' do
        let(:generation_ids) { [1, 3] }

        it 'clears local offsets' do
          expect(offset_manager).to receive(:clear_offsets)

          expect do
            consumer.each_message do |message|
              consumer.stop
            end
          end.to change { current_offsets['greetings'].keys }.from([0]).to([1])
        end
      end
    end
  end

  describe "#commit_offsets" do
    it "delegates to offset_manager" do
      expect(offset_manager).to receive(:commit_offsets)
      consumer.commit_offsets
    end
  end

  describe "#each_batch" do
    let(:messages) {
      [
        double(:message, {
          value: "hello",
          key: nil,
          headers: {},
          topic: "greetings",
          partition: 0,
          offset: 13,
          create_time: Time.now,
          is_control_record: false
        })
      ]
    }

    let(:fetched_batches) {
      [
        Kafka::FetchedBatch.new(
          topic: "greetings",
          partition: 0,
          last_offset: 13,
          highwater_mark_offset: 42,
          messages: messages,
        )
      ]
    }

    it "does not mark as processed when automatically_mark_as_processed is false" do
      expect(offset_manager).not_to receive(:mark_as_processed)

      consumer.each_batch(automatically_mark_as_processed: false) do |message|
        consumer.stop
      end
    end

    it "raises ProcessingError if the processing code fails" do
      expect {
        consumer.each_batch do |message|
          raise "yolo"
        end
      }.to raise_exception(Kafka::ProcessingError) {|exception|
        expect(exception.topic).to eq "greetings"
        expect(exception.partition).to eq 0
        expect(exception.offset).to eq 13..13

        cause = exception.cause

        expect(cause.class).to eq RuntimeError
        expect(cause.message).to eq "yolo"
      }

      expect(log.string).to include "Exception raised when processing greetings/0 in offset range 13..13 -- RuntimeError: yolo"
    end

    context 'message from #fetch_batches is old, and from a partition not assigned to this consumer.' do
      include_context 'from unassigned partition'

      it 'does not update offsets for messages from unassigned partitions' do
        consumer.each_batch do |batch|
          consumer.stop
        end

        expect(offset_manager).to have_received(:commit_offsets_if_necessary).twice

        offsets = consumer.instance_variable_get(:@current_offsets)
        expect(offsets['greetings'].keys).not_to include(1)
      end

      it 'does not process messages from unassigned partitions' do
        @yield_count = 0

        consumer.each_batch do |batch|
          batch.messages.each do |message|
            @yield_count += 1
          end
          consumer.stop
        end

        expect(offset_manager).to have_received(:commit_offsets_if_necessary).twice
        expect(@yield_count).to eq 1
      end
    end
  end

  describe "#seek" do
    it "looks for the given topic-partition-offset" do
      expect(offset_manager).to receive(:seek_to).with("greetings", 0, 14)

      consumer.seek("greetings", 0, 14)
    end
  end

  describe "#trigger_heartbeat" do
    it "sends heartbeat if necessary" do
      expect(heartbeat).to receive(:trigger)
      consumer.trigger_heartbeat
    end
  end

  describe "#trigger_heartbeat!" do
    it "always sends heartbeat" do
      expect(heartbeat).to receive(:trigger!)
      consumer.trigger_heartbeat!
    end
  end

  describe '#send_heartbeat_if_necessary' do
    subject(:method_original_name) { consumer.method(:send_heartbeat_if_necessary).original_name }

    it { expect(method_original_name).to eq(:trigger_heartbeat) }
  end

  describe '#send_heartbeat' do
    subject(:method_original_name) { consumer.method(:send_heartbeat).original_name }

    it { expect(method_original_name).to eq(:trigger_heartbeat!) }
  end
end
