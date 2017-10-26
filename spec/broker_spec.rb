require "kafka/protocol/message"

describe Kafka::Broker do
  let(:logger) { LOGGER }
  let(:connection) { FakeConnection.new }
  let(:broker) { Kafka::Broker.new(connection: connection, logger: logger) }

  class FakeConnection
    def initialize
      @mocked_response = nil
    end

    def mock_response(response)
      @mocked_response = response
    end

    def send_request(request)
      @mocked_response
    end
  end

  describe "#address_match?" do
    it "delegates to @connection" do
      host = "test_host"
      port = 333
      connection = instance_double(Kafka::Connection)
      allow(connection).to receive(:address_match?).with(host, port) { true }

      broker = Kafka::Broker.new(connection: connection, logger: logger)

      expect(broker.address_match?(host, port)).to be_truthy
    end
  end

  describe "#metadata" do
    it "fetches cluster metadata" do
      response = Kafka::Protocol::MetadataResponse.new(brokers: [], topics: [])
      connection.mock_response(response)

      metadata = broker.fetch_metadata(topics: [])

      expect(metadata).to eq response
    end
  end

  describe "#produce" do
    let(:message) { Kafka::Protocol::Message.new(key: "yo", value: "lo") }

    it "waits for a response if acknowledgements are required" do
      response = Kafka::Protocol::ProduceResponse.new
      connection.mock_response(response)

      actual_response = broker.produce(
        required_acks: -1, # -1 means all replicas must ack
        timeout: 1,
        messages_for_topics: {
          "yolos" => {
            3 => [message],
          }
        }
      )

      expect(actual_response).to eq response
    end

    it "doesn't wait for a response if zero acknowledgements are required" do
      response = broker.produce(
        required_acks: 0, # 0 means the server doesn't respond or ack at all
        timeout: 1,
        messages_for_topics: {
          "yolos" => {
            3 => [message],
          }
        }
      )

      expect(response).to be_nil
    end
  end

  describe "#fetch_messages" do
    it "fetches messages from the specified topic/partition" do
      response = Kafka::Protocol::ProduceResponse.new

      connection.mock_response(response)

      actual_response = broker.fetch_messages(
        max_wait_time: 0,
        min_bytes: 0,
        topics: {}
      )

      expect(actual_response.topics).to eq []
    end
  end
end
