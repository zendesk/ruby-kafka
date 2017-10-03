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

  it "logs errors by default" do
    expect(logger).to receive(:error).with(ArgumentError)
    expect(logger).to receive(:error).with("I'm not sure what you're doing here.")
    expect(logger).to receive(:error).with("line1\nline2\nline3")

    error = ArgumentError.new("I'm not sure what you're doing here.")
    error.set_backtrace(["line1", "line2", "line3"])

    described_class::DEFAULT_ERROR_HANDLER.call(error, logger, nil)
  end

  it "uses the default error handler when no other error handler is provided" do
    async_producer = Kafka::AsyncProducer.new(
      sync_producer: sync_producer,
      instrumenter: instrumenter,
      logger: logger,
    )

    expect(async_producer.instance_variable_get(:@error_handler)).to eq(described_class::DEFAULT_ERROR_HANDLER)
  end

  it "can use a custom error handler" do
    error_was_handled = false
    error_handler = lambda { |_logger, _error, _payload| error_was_handled = true }

    async_producer = Kafka::AsyncProducer.new(
      sync_producer: sync_producer,
      instrumenter: instrumenter,
      logger: logger,
      error_handler: error_handler,
    )

    allow(sync_producer).to receive(:buffer_size) { 42 }
    allow(sync_producer).to receive(:deliver_messages) { raise Kafka::Error, "uh-oh!" }
    async_producer.produce("hello", topic: "greetings", error_handler: error_handler)
    async_producer.shutdown

    expect(error_was_handled).to eq(true)
  end

  describe "#shutdown" do
    let(:async_producer) {
      Kafka::AsyncProducer.new(
        sync_producer: sync_producer,
        instrumenter: instrumenter,
        logger: logger,
      )
    }

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
end
