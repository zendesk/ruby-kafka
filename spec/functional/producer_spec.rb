describe "Producer API", functional: true do
  let(:logger) { Logger.new(LOG) }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.producer(max_retries: 1, retry_backoff: 0) }

  before do
    require "test_cluster"
  end

  after do
    producer.shutdown
  end

  example "writing messages using the buffered producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: "test-messages", partition: 0)
    producer.produce(value2, key: "y", topic: "test-messages", partition: 1)

    producer.deliver_messages

    message1 = kafka.fetch_messages(topic: "test-messages", partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: "test-messages", partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "having the producer assign partitions based on partition keys" do
    producer.produce("hello1", key: "x", topic: "test-messages", partition_key: "xk")
    producer.produce("hello2", key: "y", topic: "test-messages", partition_key: "yk")

    producer.deliver_messages
  end

  example "having the producer assign partitions based on message keys" do
    producer.produce("hello1", key: "x", topic: "test-messages")
    producer.produce("hello2", key: "y", topic: "test-messages")

    producer.deliver_messages
  end

  example "omitting message keys entirely" do
    producer.produce("hello1", topic: "test-messages")
    producer.produce("hello2", topic: "test-messages")

    producer.deliver_messages
  end

  example "writing to a an explicit partition of a topic that doesn't yet exist" do
    topic = "topic#{rand(1000)}"

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic, partition: 0)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example "writing to a an unspecified partition of a topic that doesn't yet exist" do
    topic = "topic#{rand(1000)}"

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example "handle a broker going down after the initial discovery" do
    begin
      producer = kafka.producer(max_retries: 10, retry_backoff: 1)

      KAFKA_CLUSTER.kill_kafka_broker(0)

      # Write to all partitions so that we'll be sure to hit the broker.
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 0)
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 1)
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 2)

      producer.deliver_messages
    ensure
      KAFKA_CLUSTER.start_kafka_broker(0)
    end
  end
end
