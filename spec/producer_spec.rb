describe Kafka::Producer do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:cluster) { FakeCluster.new }

  class FakeCluster
    attr_reader :requests

    def initialize
      @requests = []
    end

    def produce(**options)
      @requests << [:produce, options]
    end
  end

  it "buffers messages and sends them in bulk" do
    producer = Kafka::Producer.new(cluster: cluster, logger: logger)

    producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
    producer.write("hello2", key: "y", topic: "test-messages", partition: 1)

    expect(cluster.requests).to be_empty

    producer.flush

    expect(cluster.requests.size).to eq 1

    request_type, request = cluster.requests.first

    expect(request).to eq({
      required_acks: 1,
      timeout: 10_000,
      messages_for_topics: {
        "test-messages" => {
          0 => [Kafka::Protocol::Message.new(key: "x", value: "hello1")],
          1 => [Kafka::Protocol::Message.new(key: "y", value: "hello2")],
        }
      }
    })
  end
end
