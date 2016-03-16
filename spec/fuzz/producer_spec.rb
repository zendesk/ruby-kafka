describe "Producing a lot of messages with an unreliable cluster", fuzz: true do
  let(:logger) { LOGGER }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }
  let(:producer) { kafka.producer(max_retries: 20, retry_backoff: 5) }

  before do
    require "test_cluster"

    logger.level = Logger::INFO

    KAFKA_CLUSTER.create_topic("fuzz", num_partitions: 10, num_replicas: 2)

    thread = Thread.new do
      loop do
        sleep 40
        broker = rand(3) # 0-2

        puts
        puts "======== KILL! ========="
        puts

        KAFKA_CLUSTER.kill_kafka_broker(broker)

        sleep 20

        KAFKA_CLUSTER.start_kafka_broker(broker)
      end
    end

    thread.abort_on_exception = true
  end

  after do
    producer.shutdown
  end

  example do
    n = 1_000_000
    publish_interval = 100

    n.times do |i|
      producer.produce("message#{i}", key: i.to_s, topic: "fuzz")

      if i % publish_interval == 0
        producer.deliver_messages
      end
    end
  end
end
