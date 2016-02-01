describe "Producer API", functional: true do
  let(:logger) { Logger.new(log) }
  let(:log) { LOG }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.get_producer(max_retries: 1, retry_backoff: 0) }

  before do
    require "test_cluster"
  end

  after do
    producer.shutdown
  end

  example "listing all topics in the cluster" do
    expect(kafka.topics).to include "test-messages"
  end

  example "writing messages using the buffered producer" do
    producer.produce("hello1", key: "x", topic: "test-messages", partition: 0)
    producer.produce("hello2", key: "y", topic: "test-messages", partition: 1)

    producer.send_messages
  end

  example "having the producer assign partitions based on partition keys" do
    producer.produce("hello1", key: "x", topic: "test-messages", partition_key: "xk")
    producer.produce("hello2", key: "y", topic: "test-messages", partition_key: "yk")

    producer.send_messages
  end

  example "having the producer assign partitions based on message keys" do
    producer.produce("hello1", key: "x", topic: "test-messages")
    producer.produce("hello2", key: "y", topic: "test-messages")

    producer.send_messages
  end

  example "omitting message keys entirely" do
    producer.produce("hello1", topic: "test-messages")
    producer.produce("hello2", topic: "test-messages")

    producer.send_messages
  end

  example "handle a broker going down after the initial discovery" do
    begin
      producer = kafka.get_producer(max_retries: 3, retry_backoff: 5)

      KAFKA_CLUSTER.kill_kafka_broker(0)

      # Write to all partitions so that we'll be sure to hit the broker.
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 0)
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 1)
      producer.produce("hello1", key: "x", topic: "test-messages", partition: 2)

      producer.send_messages
    ensure
      KAFKA_CLUSTER.start_kafka_broker(0)
    end
  end
end
