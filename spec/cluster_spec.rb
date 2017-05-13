describe Kafka::Cluster do
  describe "#initialize" do
    let(:seed_brokers) { ["test1:9092"] }
    let(:broker_pool) { double(:broker_pool) }
    let(:logger) { Logger.new(LOG) }

    subject {
      Kafka::Cluster.new(
        seed_brokers: seed_brokers,
        broker_pool: broker_pool,
        logger: logger
      )
    }

    context "when logger is not nil" do
      it "sets the logger and does not warn" do
        expect {
          subject
        }.to_not output.to_stderr
        set_logger = subject.instance_variable_get("@logger")
        expect(set_logger).to be_a Logger
        expect(set_logger.instance_variable_get("@logdev")).to_not be_nil
      end
    end

    context "when logger is nil" do
      let(:logger) { nil }

      it "sets a no output logger and warns about no logging" do
        expect {
          subject
        }.to output("No logger was configured, defaulting to not log output.\n").to_stderr
        set_logger = subject.instance_variable_get("@logger")
        expect(set_logger).to be_a Logger
        expect(set_logger.instance_variable_get("@logdev")).to be_nil
      end
    end
  end

  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:broker_pool) { double(:broker_pool) }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              Kafka::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,
                partition_error_code: 5, # <-- this is the important bit.
              )
            ]
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::LeaderNotAvailable
    end

    it "raises InvalidTopic if the topic is invalid" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            topic_error_code: 17, # <-- this is the important bit.
            partitions: []
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = Kafka::Cluster.new(
        seed_brokers: [URI("kafka://not-there:9092"), URI("kafka://not-here:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )

      allow(broker_pool).to receive(:connect).and_raise(Kafka::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(Kafka::ConnectionError)
    end
  end
end
