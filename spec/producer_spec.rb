describe Kafka::Producer do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:broker) { FakeBroker.new }
  let(:producer) { Kafka::Producer.new(broker: broker, logger: logger) }

  class FakeBroker
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

    expect(broker.requests).to be_empty

    producer.flush

    expect(broker.requests.size).to eq 1

    request_type, request = broker.requests.first

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

    broker.mock_response(response)
  end
end
