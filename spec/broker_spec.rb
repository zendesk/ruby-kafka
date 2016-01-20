require "kafka/protocol/message"

describe Kafka::Broker do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:host) { KAFKA_HOST }
  let(:port) { KAFKA_PORT }

  let(:broker) do
    Kafka::Broker.connect(
      host: host,
      port: port,
      client_id: "test-#{rand(1000)}",
      logger: logger,
    )
  end

  describe "#metadata" do
    it "fetches cluster metadata" do
      metadata = broker.fetch_metadata(topics: [])

      brokers = metadata.brokers

      expect(brokers.size).to eq 1

      expect(brokers.first.host).to eq host
      expect(brokers.first.port).to eq port
    end
  end

  describe "#produce" do
    let(:topic) { "test-messages" }
    let(:message) { Kafka::Protocol::Message.new(key: "yo", value: "lo") }

    it "sends message sets to the broker" do
      response = broker.produce(
        required_acks: -1, # -1 means all replicas must ack
        timeout: 1000,
        messages_for_topics: { topic => { 0 => [message] } }
      )

      topic_info = response.topics.first
      partition_info = topic_info.partitions.first

      expect(topic_info.topic).to eq topic
      expect(partition_info.partition).to eq 0
      expect(partition_info.offset).to be > 0
    end

    it "doesn't wait for a response if zero acknowledgements are required" do
      response = broker.produce(
        required_acks: 0, # 0 means the server doesn't respond or ack at all
        timeout: 1000,
        messages_for_topics: { topic => { 0 => [message] } }
      )

      expect(response).to be_nil
    end
  end
end
