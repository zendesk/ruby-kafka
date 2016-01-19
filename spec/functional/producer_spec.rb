describe "Producer API" do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:host) { KAFKA_HOST }
  let(:port) { KAFKA_PORT }

  let(:cluster) do
    Kafka::Cluster.connect(
      brokers: ["#{host}:#{port}"],
      client_id: "test-#{rand(1000)}",
      logger: logger,
    )
  end

  example "writing messages using the buffered producer" do
    producer = Kafka::Producer.new(cluster: cluster, logger: logger)

    producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
    producer.write("hello2", key: "y", topic: "test-messages", partition: 1)

    producer.flush
  end
end
