# frozen_string_literal: true

describe Kafka::Cluster do
  let(:broker) { double(:broker) }
  let(:broker_pool) { double(:broker_pool) }

  let(:cluster) {
    Kafka::Cluster.new(
      seed_brokers: [URI("kafka://test1:9092")],
      broker_pool: broker_pool,
      logger: LOGGER,
    )
  }

  describe "#get_leader" do
    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
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
          Kafka::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
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

  describe "#add_target_topics" do
    it "raises ArgumentError if the topic is nil" do
      expect {
        cluster.add_target_topics([nil])
      }.to raise_exception(ArgumentError)
    end

    it "raises ArgumentError if the topic is empty" do
      expect {
        cluster.add_target_topics([""])
      }.to raise_exception(ArgumentError)
    end
  end

  describe "#cluster_info" do
    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
        resolve_seed_brokers: resolve_seed_brokers,
      )
    }

    before do
      allow(broker).to receive(:fetch_metadata) { raise Kafka::ConnectionError, "Operation timed out" }
      allow(broker).to receive(:disconnect)
    end

    context "when resolve_seed_brokers is false" do
      let(:resolve_seed_brokers) { false }

      it "tries the seed broker hostnames as is" do
        expect(broker_pool).to receive(:connect).with("test1", 9092) { broker }
        expect {
          cluster.cluster_info
        }.to raise_error(Kafka::ConnectionError, %r{kafka://test1:9092: Operation timed out})
      end
    end

    context "when resolve_seed_brokers is true" do
      let(:resolve_seed_brokers) { true }

      before do
        allow(Resolv).to receive(:getaddresses) { ["127.0.0.1", "::1"] }
      end

      it "tries all the resolved IP addresses" do
        expect(broker_pool).to receive(:connect).with("127.0.0.1", 9092) { broker }
        expect(broker_pool).to receive(:connect).with("::1", 9092) { broker }
        expect {
          cluster.cluster_info
        }.to raise_error(Kafka::ConnectionError, %r{kafka://test1:9092 \(127\.0\.0\.1\): Operation timed out})
      end
    end
  end
end
