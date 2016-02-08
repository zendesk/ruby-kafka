describe Kafka::BrokerPool do
  describe "#get_leader" do
    let(:broker) { double(:broker) }

    let(:pool) {
      Kafka::BrokerPool.new(
        seed_brokers: ["test1:9092"],
        client_id: "test",
        logger: Logger.new(LOG),
      )
    }

    before do
      allow(Kafka::Broker).to receive(:connect) { broker }
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
        pool.get_leader("greetings", 42)
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
        pool.get_leader("greetings", 42)
      }.to raise_error Kafka::InvalidTopic
    end
  end
end
