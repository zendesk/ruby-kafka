# frozen_string_literal: true

require "kafka/protocol/message"

describe Kafka::Broker do
  let(:logger) { LOGGER }
  let(:connection) { FakeConnection.new }
  let(:connection_builder) { double(:connection_builder) }

  before do
    allow(connection_builder).to receive(:build_connection) { connection }
  end

  let(:broker) {
    Kafka::Broker.new(
      connection_builder: connection_builder,
      host: "x.com",
      port: 9092,
      node_id: 1,
      logger: logger,
    )
  }

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

    def close
    end
  end

  describe "#address_match?" do
    it "delegates to @connection" do
      host = "test_host"
      port = 333

      broker = Kafka::Broker.new(
        connection_builder: connection_builder,
        host: host,
        port: port,
        node_id: 1,
        logger: logger,
      )

      expect(broker.address_match?(host, port)).to be_truthy
    end
  end

  describe "#metadata" do
    it "fetches cluster metadata" do
      response = Kafka::Protocol::MetadataResponse.new(brokers: [], controller_id: nil, topics: [])
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
        max_bytes: 10 * 1024,
        topics: {}
      )

      expect(actual_response.topics).to eq []
    end
  end

  describe "#disconnect" do
    it "doesn't close a connection if it's not connected yet " do
      expect(connection).not_to receive(:close)
      broker.disconnect
    end

    it "closes a connection if the connection is present" do
      expect(connection).to receive(:close)

      broker.fetch_messages(
        max_wait_time: 0, min_bytes: 0, max_bytes: 10 * 1024, topics: {}
      )

      broker.disconnect
    end
  end

  describe "#add_offsets_to_txn" do
    it 'sends offsets to the transaction' do
      allow(connection).to receive(:send_request)
      broker.add_offsets_to_txn(
        transactional_id: 1, producer_id: 2, producer_epoch: 3, group_id: 4
      )
      expect(connection).to have_received(:send_request)

      broker.disconnect
    end
  end

  describe "#txn_offset_commit" do
    before do
      allow(connection).to receive(:send_request)
    end
    it 'commits transaction' do
      broker.txn_offset_commit(
        transactional_id: 1, producer_id: 2, producer_epoch: 3, group_id: 4, offsets: []
      )
      expect(connection).to have_received(:send_request)
      broker.disconnect
    end
  end
end
