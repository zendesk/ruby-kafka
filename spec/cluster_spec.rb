require "kafka/protocol/message"

describe Kafka::Cluster do
  let(:log) { StringIO.new }
  let(:log) { $stderr }
  let(:logger) { Logger.new(log) }
  let(:host) { KAFKA_HOST }
  let(:port) { KAFKA_PORT }

  let(:cluster) do
    Kafka::Cluster.connect(
      brokers: ["#{host}:#{port}"],
      client_id: "test-#{rand(1000)}",
      logger: logger,
    )
  end

  describe "#metadata" do
    it "fetches cluster metadata" do
      metadata = cluster.fetch_metadata(topics: [])

      brokers = metadata.brokers

      expect(brokers.size).to eq 1

      expect(brokers.first.host).to eq host
      expect(brokers.first.port).to eq port
    end
  end

  describe "#produce" do
    it "sends message sets to the broker" do
      topic = "test-messages"

      response = cluster.produce(
        required_acks: 0,
        timeout: 1000,
        messages_for_topics: {
          topic => {
            0 => [
              Kafka::Protocol::Message.new(key: "yo", value: "lo"),
            ]
          }
        }
      )

      topic_info = response.topics.first

      expect(topic_info.topic).to eq topic
    end
  end
end
