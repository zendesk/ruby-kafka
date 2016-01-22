describe "Producer API" do
  let(:logger) { Logger.new(log) }
  let(:log) { LOG }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.get_producer }

  after do
    producer.shutdown
  end

  example "writing messages using the buffered producer" do
    producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
    producer.write("hello2", key: "y", topic: "test-messages", partition: 1)

    producer.flush
  end

  example "having the producer assign partitions based on partition keys" do
    producer.write("hello1", key: "x", topic: "test-messages", partition_key: "xk")
    producer.write("hello2", key: "y", topic: "test-messages", partition_key: "yk")

    producer.flush
  end

  example "having the producer assign partitions based on message keys" do
    producer.write("hello1", key: "x", topic: "test-messages")
    producer.write("hello2", key: "y", topic: "test-messages")

    producer.flush
  end
end
