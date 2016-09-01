require "timecop"

describe Kafka::Consumer do
  let(:cluster) { double(:cluster) }
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }
  let(:group) { double(:group) }
  let(:offset_manager) { double(:offset_manager) }
  let(:heartbeat) { double(:heartbeat) }
  let(:fetch_operation) { double(:fetch_operation) }
  let(:session_timeout) { 30 }
  let(:assigned_partitions) { { "greetings" => [0] } }

  let(:consumer) {
    Kafka::Consumer.new(
      cluster: cluster,
      logger: logger,
      instrumenter: instrumenter,
      group: group,
      offset_manager: offset_manager,
      session_timeout: session_timeout,
      heartbeat: heartbeat,
    )
  }

  before do
    allow(Kafka::FetchOperation).to receive(:new) { fetch_operation }

    allow(cluster).to receive(:add_target_topics)
    allow(cluster).to receive(:refresh_metadata_if_necessary!)

    allow(offset_manager).to receive(:commit_offsets)
    allow(offset_manager).to receive(:commit_offsets_if_necessary)
    allow(offset_manager).to receive(:set_default_offset)
    allow(offset_manager).to receive(:mark_as_processed)
    allow(offset_manager).to receive(:next_offset_for) { 42 }

    allow(group).to receive(:subscribe)
    allow(group).to receive(:leave)
    allow(group).to receive(:member?) { true }
    allow(group).to receive(:assigned_partitions) { assigned_partitions }

    allow(heartbeat).to receive(:send_if_necessary)

    allow(fetch_operation).to receive(:fetch_from_partition)
    allow(fetch_operation).to receive(:execute) { fetched_batches }

    consumer.subscribe("greetings")
  end

  describe "#each_message" do
    let(:messages) {
      [
        Kafka::FetchedMessage.new(
          value: "hello",
          key: nil,
          topic: "greetings",
          partition: 0,
          offset: 13,
          create_time: Time.now,
        )
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

    it "seeks to the default offset when the checkpoint is invalid " do
      done = false

      allow(offset_manager).to receive(:seek_to_default) { done = true }

      allow(fetch_operation).to receive(:execute) {
        if done
          fetched_batches
        else
          raise Kafka::OffsetOutOfRange
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

      expect(fetch_operation).to_not have_received(:fetch_from_partition).with("greetings", 42, anything)

      consumer.resume("greetings", 42)

      consumer.each_message do |message|
        consumer.stop
      end

      expect(fetch_operation).to have_received(:fetch_from_partition).with("greetings", 42, anything)
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
  end
end
