describe Kafka::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:connection) { double(:connection) }
    let(:connection_pool) { double(:connection_pool) }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: ["test1:9092"],
        connection_pool: connection_pool,
        logger: Logger.new(LOG),
      )
    }

    before do
      allow(connection_pool).to receive(:connect) { connection }
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

      allow(connection).to receive(:send_request) { metadata }

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

      allow(connection).to receive(:send_request) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = Kafka::Cluster.new(
        seed_brokers: ["not-there:9092", "not-here:9092"],
        connection_pool: connection_pool,
        logger: Logger.new(LOG),
      )

      allow(connection_pool).to receive(:connect).and_raise(Kafka::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(Kafka::ConnectionError)
    end
  end
end
