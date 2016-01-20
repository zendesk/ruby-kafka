describe "Producer API" do
  let(:logger) { Logger.new(log) }
  let(:log) { StringIO.new }
  let(:broker_pool) { Kafka::BrokerPool.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }

  example "writing messages using the buffered producer" do
    producer = Kafka::Producer.new(broker_pool: broker_pool, logger: logger)

    producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
    producer.write("hello2", key: "y", topic: "test-messages", partition: 1)

    producer.flush
  end
end
