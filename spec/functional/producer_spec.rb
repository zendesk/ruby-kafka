# frozen_string_literal: true

describe "Producer API", functional: true do
  let(:producer) { kafka.producer(max_retries: 3, retry_backoff: 1) }

  after do
    producer.shutdown
  end

  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "setting a create_time value" do
    timestamp = Time.now

    producer.produce("hello", topic: topic, partition: 0, create_time: timestamp)
    producer.deliver_messages

    message = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last

    expect(message.create_time.to_i).to eq timestamp.to_i
  end

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
end
