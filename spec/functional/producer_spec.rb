describe "Producer API", functional: true do
  let(:producer) { kafka.producer(max_retries: 3, retry_backoff: 1) }

  after do
    producer.shutdown
  end

  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "writing messages using the buffered producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: topic, partition: 0)
    producer.produce(value2, key: "y", topic: topic, partition: 1)

    producer.deliver_messages

    message1 = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "having the producer assign partitions based on partition keys" do
    producer.produce("hello1", key: "x", topic: topic, partition_key: "xk")
    producer.produce("hello2", key: "y", topic: topic, partition_key: "yk")

    producer.deliver_messages
  end

  example "having the producer assign partitions based on message keys" do
    producer.produce("hello1", key: "x", topic: topic)
    producer.produce("hello2", key: "y", topic: topic)

    producer.deliver_messages
  end

  example "omitting message keys entirely" do
    producer.produce("hello1", topic: topic)
    producer.produce("hello2", topic: topic)

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
    topic = create_random_topic(num_partitions: 3, num_replicas: 2)

    begin
      producer = kafka.producer(max_retries: 30, retry_backoff: 2)

      KAFKA_CLUSTER.kill_kafka_broker(0)

      # Write to all partitions so that we'll be sure to hit the broker.
      producer.produce("hello1", key: "x", topic: topic, partition: 0)
      producer.produce("hello1", key: "x", topic: topic, partition: 1)
      producer.produce("hello1", key: "x", topic: topic, partition: 2)

      producer.deliver_messages
    ensure
      KAFKA_CLUSTER.start_kafka_broker(0)
    end
  end

  example "sending a message that's too large for Kafka to handle" do
    producer.produce("hello", topic: "test-messages", partition: 0)
    producer.produce("x" * 1_000_000, topic: "test-messages", partition: 0)
    producer.produce("goodbye", topic: "test-messages", partition: 0)

    expect {
      producer.deliver_messages
    }.to raise_exception(Kafka::DeliveryFailed)
  end
end
