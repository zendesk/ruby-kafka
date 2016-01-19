describe Kafka::Producer do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:cluster) { FakeCluster.new }
  let(:producer) { Kafka::Producer.new(cluster: cluster, logger: logger) }

  class FakeCluster
    attr_reader :requests

    def initialize
      @requests = []
      @mock_response = nil
    end

    def produce(**options)
      @requests << [:produce, options]
      @mock_response
    end

    def mock_response(response)
      @mock_response = response
    end
  end

  it "buffers messages and sends them in bulk" do
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

  it "raises an error if a message is corrupt" do
    mock_response_with_error_code(2)

    expect { producer.flush }.to raise_error(Kafka::CorruptMessage)
  end

  it "raises an error if the topic or partition are not known" do
    mock_response_with_error_code(3)

    expect { producer.flush }.to raise_error(Kafka::UnknownTopicOrPartition)
  end

  def mock_response_with_error_code(error_code)
    response = Kafka::Protocol::ProduceResponse.new(
      topics: [
        Kafka::Protocol::ProduceResponse::TopicInfo.new(
          topic: "test-messages",
          partitions: [
            Kafka::Protocol::ProduceResponse::PartitionInfo.new(
              partition: 0,
              offset: 0,
              error_code: error_code,
            )
          ]
        )
      ]
    )

    cluster.mock_response(response)
  end
end
