describe "Producer API", type: :functional do
  let(:logger) { Logger.new(log) }
  let(:log) { LOG }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.get_producer }

  before do
    require "test_cluster"
  end

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

  example "omitting message keys entirely" do
    producer.write("hello1", topic: "test-messages")
    producer.write("hello2", topic: "test-messages")

    producer.flush
  end

  example "handle a broker going down after the initial discovery" do
    begin
      producer

      KAFKA_CLUSTER.kill_kafka_broker(0)

      producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
      producer.write("hello1", key: "x", topic: "test-messages", partition: 1)
      producer.write("hello1", key: "x", topic: "test-messages", partition: 2)

      producer.flush
    ensure
      KAFKA_CLUSTER.start_kafka_broker(0)
    end
  end
end
