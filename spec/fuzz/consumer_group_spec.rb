describe "Consumer groups", fuzz: true do
  let(:logger) { Logger.new(LOG) }
  let(:num_messages) { 750_000 }
  let(:num_partitions) { 30 }
  let(:num_consumers) { 10 }
  let(:topic) { "fuzz-consumer-group" }

  before do
    require "test_cluster"

    logger.level = Logger::INFO

    KAFKA_CLUSTER.create_topic(topic, num_partitions: num_partitions, num_replicas: 1)

    kafka = Kafka.new(seed_brokers: KAFKA_BROKERS, logger: logger)
    producer = kafka.producer(max_buffer_size: 5000)

    (1..num_messages).each do |i|
      producer.produce("message#{i}", topic: topic, partition: i % num_partitions)
      producer.deliver_messages if i % 3000 == 0
    end

    producer.deliver_messages
  end

  example "consuming messages in a group with unreliable members" do
    result_queue = Queue.new

    consumer_threads = num_consumers.times.map do
      thread = Thread.new do
        kafka = Kafka.new(seed_brokers: KAFKA_BROKERS, logger: logger)
        consumer = kafka.consumer(group_id: "fuzz")
        consumer.subscribe(topic)

        consumer.each_message do
          result_queue << :ok
        end
      end

      thread.abort_on_exception = true

      thread
    end

    expect {
      num_messages.times { result_queue.deq }
    }.to_not raise_exception
  end
end
