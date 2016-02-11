describe Kafka::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:client) { double(:client) }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: ["test1:9092"],
        client: client,
        logger: Logger.new(LOG),
      )
    }

    before do
      allow(client).to receive(:connect_to).with("test1", 9092) { broker }
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
  end
end
