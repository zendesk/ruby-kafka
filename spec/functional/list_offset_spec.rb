describe "List Offset API", functional: true do
  let(:logger) { LOGGER }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.producer(max_retries: 1, retry_backoff: 0) }

  before do
    require "test_cluster"

    value1 = rand(10_000).to_s
    producer.produce(value1, key: "x", topic: "test-messages", partition: 0)
    producer.produce(value1, key: "x", topic: "test-messages", partition: 0)
    producer.deliver_messages
  end

  after do
    kafka.close
  end

  example "List the earliest offset for a topic/partitian" do
    expected = kafka.list_offset(topic: "test-messages", partition: 0, offset: :earliest)

    expect(expected).to eq(0)
  end

  example "List the latest offset for a topic/partitian" do
    expected = kafka.list_offset(topic: "test-messages", partition: 0, offset: :latest)

    expect(expected).to eq(2)
  end
end
