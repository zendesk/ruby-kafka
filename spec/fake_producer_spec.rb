require "kafka/fake_producer"

describe Kafka::FakeProducer do
  it "allows reading messages that have been delivered" do
    producer = Kafka::FakeProducer.new

    producer.produce("hello", topic: "greetings", partition: 0)
    producer.produce("world", topic: "greetings", partition: 0)

    messages = producer.messages_in("greetings")

    expect(messages.map(&:value)).to eq ["hello", "world"]
    expect(messages.map(&:offset)).to eq [0, 1]
  end
end
