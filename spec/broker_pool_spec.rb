describe Kafka::BrokerPool do
  let(:connection_builder) do
    builder = double()
    allow(builder).to receive(:build_connection)
    builder
  end
  let(:logger) { LOGGER }
  let(:broker_pool) do
    described_class.new(
      connection_builder: connection_builder,
      logger: logger,
    )
  end
  let(:host) { "localhost" }
  let(:port) { 9092 }
  let(:node_id) { 0 }

  before do
    allow(Kafka::Broker).to receive(:new).and_call_original
  end

  describe "#connect" do
    it "creates a new broker and does not set cache if node_id nil" do
      broker_pool.connect(host, port)

      expect(connection_builder).to have_received(:build_connection).with(host, port)
      expect(Kafka::Broker).to have_received(:new).once
      brokers = broker_pool.instance_variable_get(:@brokers)
      expect(brokers).to be_empty
    end

    it "creates and caches new broker if node_id given" do
      broker = broker_pool.connect(host, port, node_id: node_id)

      expect(connection_builder).to have_received(:build_connection).with(host, port)
      expect(Kafka::Broker).to have_received(:new).once
      brokers = broker_pool.instance_variable_get(:@brokers)
      expected_brokers = {
        node_id => { broker: broker, uri: [host, port].join(":") }
      }
      expect(brokers).to eq(expected_brokers)
    end

    it "uses cached broker if node_id, host, and port match" do
      # Setting cache
      cached_broker = broker_pool.connect(host, port, node_id: node_id)
      # Fetch from cache
      broker = broker_pool.connect(host, port, node_id: node_id)

      expect(Kafka::Broker).to have_received(:new).once
      expect(broker).to eq(cached_broker)
    end

    it "disconnects stale cached broker connections" do
      # Setting cache
      cached_broker = broker_pool.connect(host, port, node_id: node_id)
      allow(cached_broker).to receive(:disconnect)

      # Replaces cache since node_id are the same
      new_host = "broker.kafka.com"
      broker = broker_pool.connect(new_host, port, node_id: node_id)

      expect(Kafka::Broker).to have_received(:new).twice
      expect(broker).not_to eq(cached_broker)
      expect(cached_broker).to have_received(:disconnect).once
      brokers = broker_pool.instance_variable_get(:@brokers)
      expected_brokers = {
        node_id => { broker: broker, uri: [new_host, port].join(":") }
      }
      expect(brokers).to eq(expected_brokers)
    end
  end

  describe "#close" do
    it "disconnects all broker connections" do
      broker1 = broker_pool.connect(host, port, node_id: node_id)
      broker2 = broker_pool.connect("new.host", port, node_id: 1)
      allow(broker1).to receive(:disconnect)
      allow(broker2).to receive(:disconnect)

      broker_pool.close

      expect(broker1).to have_received(:disconnect)
      expect(broker2).to have_received(:disconnect)
    end
  end
end
