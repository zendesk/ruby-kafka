require "kafka"

describe "Metadata request/response" do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:host) { ENV.fetch("KAFKA_HOST") }
  let(:port) { ENV.fetch("KAFKA_PORT").to_i }

  let(:connection) do
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test-#{rand(1000)}",
      logger: logger,
    )
  end

  before do
    connection.open
  end

  example "fetching cluster metadata" do
    request = Kafka::Protocol::TopicMetadataRequest.new(
      topics: []
    )

    response = Kafka::Protocol::MetadataResponse.new

    connection.write_request(request)
    connection.read_response(response)

    brokers = response.brokers

    expect(brokers.size).to eq 1

    expect(brokers.first.host).to eq host
    expect(brokers.first.port).to eq port
  end
end
