describe Kafka::BrokerPool do
  let(:connection_builder) { double('connection_builder') }
  let(:connection) { double('connection') }
  let(:logger) { LOGGER }
  let(:broker_pool) do
    described_class.new(
      connection_builder: connection_builder,
      logger: logger,
    )
  end
  let(:broker) { double('broker') }
  let(:host) { "localhost" }
  let(:port) { 9092 }
  let(:node_id) { 0 }

  before do
    allow(Kafka::Broker).to receive(:new).and_call_original
    allow(connection_builder).to receive(:build_connection).with(host, port) { connection }
  end

  describe "#connect" do
    it "creates a new broker everytime it is called with node_id nil" do
      # Call without node_id the first time.
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: nil,
        logger: logger
      }) { broker }
      expect(broker_pool.connect(host, port)).to eq(broker)

      # Call without node_id the second time returns new broker.
      second_broker = double()
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: nil,
        logger: logger
      }) { second_broker }
      expect(broker_pool.connect(host, port)).to eq(second_broker)
    end

    it "creates a new broker the first time it is called with a particular node_id" do
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      }) { broker }

      expect(broker_pool.connect(host, port, node_id: node_id)).to eq(broker)
    end

    it "does not create a new broker if address & node_id match" do
      allow(broker).to receive(:address_match?).with(host, port) { true }
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      }) { broker }

      expect(broker_pool.connect(host, port, node_id: node_id)).to eq(broker)
      expect(broker_pool.connect(host, port, node_id: node_id)).to eq(broker)
    end

    it "disconnects existing broker if new broker address does not match pre-existing broker address for a given node_id" do
      allow(broker).to receive(:address_match?).with("#{host}1", "#{port}1") { false }
      allow(broker).to receive(:disconnect).once
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      }) { broker }
      expect(broker_pool.connect(host, port, node_id: node_id)).to eq(broker)

      new_host = "#{host}1"
      new_port = "#{port}1"
      allow(connection_builder).to receive(:build_connection).with(new_host, new_port) { connection }
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      })

      broker_pool.connect(new_host, new_port, node_id: node_id)

      expect(broker).to have_received(:disconnect)
    end

    it "creates a new broker if address does not match pre-existing broker address for a given node_id" do
      allow(broker).to receive(:address_match?).with("#{host}1", "#{port}1") { false }
      allow(broker).to receive(:disconnect).once
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      }) { broker }

      new_host = "#{host}1"
      new_port = "#{port}1"
      allow(connection_builder).to receive(:build_connection).with(new_host, new_port) { connection }
      second_broker = double()
      allow(Kafka::Broker).to receive(:new).once.with({
        connection: connection,
        node_id: node_id,
        logger: logger
      }) { second_broker }

      actual_broker = broker_pool.connect(new_host, new_port, node_id: node_id)

      expect(actual_broker).to eq(second_broker)
    end
  end

  describe "#close" do
    it "disconnects all broker connections" do
      broker1 = broker_pool.connect(host, port, node_id: node_id)
      allow(connection_builder).to receive(:build_connection).with("new.host", port) { connection }
      broker2 = broker_pool.connect("new.host", port, node_id: 1)
      allow(broker1).to receive(:disconnect)
      allow(broker2).to receive(:disconnect)

      broker_pool.close

      expect(broker1).to have_received(:disconnect)
      expect(broker2).to have_received(:disconnect)
    end
  end
end
