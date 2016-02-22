require "snappy"

describe "Compression", functional: true do
  let(:logger) { Logger.new(LOG) }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }

  before do
    require "test_cluster"
  end

  example "producing and consuming snappy-compressed messages" do
    producer = kafka.producer(
      compression_codec: :snappy,
      max_retries: 0,
      retry_backoff: 0
    )

    last_offset = fetch_last_offset

    producer.produce("message1", topic: "test-messages", partition: 0)
    producer.produce("message2", topic: "test-messages", partition: 0)

    producer.deliver_messages

    messages = kafka.fetch_messages(
      topic: "test-messages",
      partition: 0,
      offset: last_offset + 1,
    )

    expect(messages.last(2).map(&:value)).to eq ["message1", "message2"]
  end

  example "producing and consuming gzip-compressed messages" do
    producer = kafka.producer(
      compression_codec: :gzip,
      max_retries: 0,
      retry_backoff: 0
    )

    last_offset = fetch_last_offset

    producer.produce("message1", topic: "test-messages", partition: 0)
    producer.produce("message2", topic: "test-messages", partition: 0)

    producer.deliver_messages

    messages = kafka.fetch_messages(
      topic: "test-messages",
      partition: 0,
      offset: last_offset + 1,
    )

    expect(messages.last(2).map(&:value)).to eq ["message1", "message2"]
  end

  def fetch_last_offset
    last_message = kafka.fetch_messages(topic: "test-messages", partition: 0, offset: 0).last
    last_message ? last_message.offset : 0
  end
end
