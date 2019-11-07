# frozen_string_literal: true

class FakeInstrumenter
  Metric = Struct.new(:name, :payload)

  def initialize
    @metrics = Hash.new {|h, k| h[k] = [] }
  end

  def metrics_for(name)
    @metrics[name]
  end

  def instrument(name, payload = {})
    yield if block_given?
    @metrics[name] << Metric.new(name, payload)
  end
end

describe Kafka::AsyncProducer do
  let(:sync_producer) { double(:sync_producer, produce: nil, shutdown: nil, deliver_messages: nil) }
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:instrumenter) { FakeInstrumenter.new }

  let(:async_producer) {
    Kafka::AsyncProducer.new(
      sync_producer: sync_producer,
      instrumenter: instrumenter,
      max_retries: 2,
      retry_backoff: 0.2,
      logger: logger,
    )
  }

  describe "#deliver_messages" do
    it "instruments the error after failing to deliver buffered messages" do
      allow(sync_producer).to receive(:buffer_size) { 42 }
      allow(sync_producer).to receive(:deliver_messages) { raise Kafka::DeliveryFailed.new("something happened", []) }

      async_producer.produce("hello", topic: "greetings")
      async_producer.deliver_messages
      sleep 0.2 # wait for worker to call deliver_messages
      async_producer.shutdown

      metric = instrumenter.metrics_for("error.async_producer").first
      expect(metric.payload[:error]).to be_a(Kafka::DeliveryFailed)
    end
  end

  describe "#shutdown" do
    it "delivers buffered messages" do
      async_producer.produce("hello", topic: "greetings")
      async_producer.shutdown

      expect(sync_producer).to have_received(:deliver_messages)
    end

    it "instruments a failure to deliver buffered messages" do
      allow(sync_producer).to receive(:buffer_size) { 42 }
      allow(sync_producer).to receive(:deliver_messages) { raise Kafka::Error, "uh-oh!" }

      async_producer.produce("hello", topic: "greetings")
      async_producer.shutdown

      expect(log.string).to include "Failed to deliver messages during shutdown: uh-oh!"

      metric = instrumenter.metrics_for("drop_messages.async_producer").first
      expect(metric.payload[:message_count]).to eq 42
    end
  end

  describe "#produce" do
    it "delivers buffered messages" do
      async_producer.produce("hello", topic: "greetings")
      sleep 0.2 # wait for worker to call produce

      expect(sync_producer).to have_received(:produce)
    end

    it "retries until configured max_retries" do
      allow(sync_producer).to receive(:produce) {raise Kafka::BufferOverflow}

      async_producer.produce("hello", topic: "greetings")
      sleep 0.3 # wait for all retries to be done

      expect(log.string).to include "Failed to asynchronously produce messages due to BufferOverflow"

      metric = instrumenter.metrics_for("error.async_producer").first
      expect(metric.payload[:error]).to be_a(Kafka::BufferOverflow)
      expect(sync_producer).to have_received(:produce).exactly(3).times
    end

    it "requires `topic` to be a String or a Symbol" do
      expect {
        async_producer.produce("hello", topic: 42)
      }.to raise_exception(ArgumentError)
    end

    it "converts `topic` to a String if `topic` is a Symbol" do
      async_producer.produce("hello", topic: :greetings)
      sleep 0.2 # wait for worker to call produce

      expect(sync_producer).to have_received(:produce).with("hello", topic: "greetings")
    end
  end
end
