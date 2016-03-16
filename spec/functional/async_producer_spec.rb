describe "Producer API", functional: true do
  let(:logger) { LOGGER }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.async_producer(max_retries: 1, retry_backoff: 0) }

  before do
    require "test_cluster"
  end

  after do
    producer.shutdown
  end

  example "writing messages using async producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: "test-messages", partition: 0)
    producer.produce(value2, key: "y", topic: "test-messages", partition: 1)

    producer.deliver_messages

    # Wait for everything to be delivered.
    producer.shutdown

    message1 = kafka.fetch_messages(topic: "test-messages", partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: "test-messages", partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "automatically delivering messages with a fixed time interval" do
    producer = kafka.async_producer(delivery_interval: 0.1)

    value = rand(10_000).to_s
    producer.produce(value, topic: "test-messages", partition: 0)

    sleep 0.2

    messages = kafka.fetch_messages(
      topic: "test-messages",
      partition: 0,
      offset: 0,
      max_wait_time: 0.1,
    )

    expect(messages.last.value).to eq value

    producer.shutdown
  end

  example "automatically delivering messages when a buffer threshold is reached" do
    producer = kafka.async_producer(delivery_threshold: 5)

    values = 5.times.map { rand(10_000).to_s }

    values.each do |value|
      producer.produce(value, topic: "test-messages", partition: 0)
    end

    sleep 0.2

    messages = kafka.fetch_messages(
      topic: "test-messages",
      partition: 0,
      offset: 0,
      max_wait_time: 0,
    )

    expect(messages.last(5).map(&:value)).to eq values

    producer.shutdown
  end
end
