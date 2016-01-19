describe Kafka::Producer do
  let(:log) { StringIO.new }
  let(:log) { $stderr }
  let(:logger) { Logger.new(log) }
  let(:connection) { FakeConnection.new }
  let(:cluster) { Kafka::Cluster.new(connection: connection, logger: logger) }

  class FakeConnection
    attr_reader :requests

    def initialize
      @requests = []
    end

    def write_request(api_key, request)
      @requests << request
    end

    def read_response(response)
      response
    end
  end

  it "buffers messages and sends them in bulk" do
    buffer = Kafka::Producer.new(cluster: cluster, logger: logger)

    buffer.write("hello1", key: "x", topic: "test-messages", partition: 0)
    buffer.write("hello2", key: "y", topic: "test-messages", partition: 1)

    expect(connection.requests).to be_empty

    buffer.flush

    expect(connection.requests.size).to eq 1

    request = connection.requests.first

    expect(request.messages_for_topics).to eq({
      "test-messages" => {
        0 => [Kafka::Protocol::Message.new(key: "x", value: "hello1")],
        1 => [Kafka::Protocol::Message.new(key: "y", value: "hello2")],
      }
    })
  end
end
