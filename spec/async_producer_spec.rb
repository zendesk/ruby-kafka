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
  let(:lambda_state) { { "has_been_called" => false } }

  let(:async_producer) {
    Kafka::AsyncProducer.new(
      sync_producer: sync_producer,
      instrumenter: instrumenter,
      max_retries: 2,
      retry_backoff: 0.2,
      logger: logger,
      finally: lambda { |messages| lambda_state["has_been_called"] = true }
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

    it "Calls the `finally` lambda if passed in and sync_producer fails." do
      allow(sync_producer).to receive(:buffer_size) { 42 }
      allow(sync_producer).to receive(:deliver_messages) { raise Kafka::DeliveryFailed.new("something happened", []) }

      async_producer.produce("hello", topic: "greetings")
      async_producer.deliver_messages
      async_producer.shutdown
      sleep 0.2 # wait for worker to call deliver_messages
      expect(lambda_state["has_been_called"]).to be(true)
    end

    it "Calls the `finally` lambda if passed in and it fails in worker." do
      # allow(async_producer.worker).to receive(:buffer_size) { 42 }
      allow(
        async_producer.instance_variable_get("@worker").instance_variable_get("@producer")
      ).to receive(:deliver_messages) { raise Kafka::DeliveryFailed.new("something happened", []) }

      async_producer.produce("hello", topic: "greetings")
      async_producer.deliver_messages
      async_producer.shutdown
      sleep 0.2 # wait for worker to call deliver_messages
      expect(lambda_state["has_been_called"]).to be(true)
    end
  end

  describe "#shutdown" do
    it "times out if a timeout is set" do
      sleep_time = 100
      allow(sync_producer).to receive(:buffer_size) { 42 }
      allow(sync_producer).to receive(:deliver_messages) { sleep(sleep_time) }

      start_time = Time.now
      async_producer.shutdown(0)
      end_time = Time.now

      expect(end_time - start_time < sleep_time)
    end

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
      expect(metric.payload[:message_count]).to eq 43 # plus the produced message
    end
  end

  describe "#produce" do
    it "delivers buffered messages" do
      async_producer.produce("hello", topic: "greetings")
      sleep 0.2 # wait for worker to call produce

      expect(sync_producer).to have_received(:produce)

      async_producer.shutdown
    end

    it "retries until configured max_retries" do
      allow(sync_producer).to receive(:produce) {raise Kafka::BufferOverflow}

      async_producer.produce("hello", topic: "greetings")
      sleep 0.3 # wait for all retries to be done

      expect(log.string).to include "Failed to asynchronously produce messages due to BufferOverflow"

      metric = instrumenter.metrics_for("error.async_producer").first
      expect(metric.payload[:error]).to be_a(Kafka::BufferOverflow)
      expect(sync_producer).to have_received(:produce).exactly(3).times

      async_producer.shutdown
    end

    it "requires `topic` to be a String" do
      expect {
        async_producer.produce("hello", topic: :topic)
      }.to raise_exception(NoMethodError, /to_str/)
    end
  end
end
