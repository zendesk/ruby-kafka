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
    )
  }

  let(:messages) {
    [
      double(:message, {
        value: "hello",
        key: nil,
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
        highwater_mark_offset: 42,
        messages: messages,
      )
    ]
  }

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
    allow(group).to receive(:leave)
    allow(group).to receive(:member?) { true }
    allow(group).to receive(:subscribed_partitions) { assigned_partitions }

    allow(heartbeat).to receive(:send_if_necessary)

    allow(fetcher).to receive(:data?) { fetched_batches.any? }
    allow(fetcher).to receive(:poll) { [:batches, fetched_batches] }

    consumer.subscribe("greetings")
  end

  describe "#each_message" do
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
  end

  describe "#commit_offsets" do
    it "delegates to offset_manager" do
      expect(offset_manager).to receive(:commit_offsets)
      consumer.commit_offsets
    end
  end

  describe "#each_batch" do
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
  end

  describe "#seek" do
    it "looks for the given topic-partition-offset" do
      expect(offset_manager).to receive(:seek_to).with("greetings", 0, 14)

      consumer.seek("greetings", 0, 14)
    end
  end

  describe "#send_heartbeat_if_necessary" do
    it "sends heartbeat if necessary" do
      expect(heartbeat).to receive(:send_if_necessary)
      consumer.send_heartbeat_if_necessary
    end
  end
end
