describe "Consumer groups", fuzz: true do
  let(:logger) { Logger.new(LOG) }
  let(:num_messages) { 100_000 }
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
    consumer_threads = num_consumers.times.map { start_consumer(result_queue) }

    nemesis = Thread.new do
      loop do
        sleep 60

        target = consumer_threads.sample
        consumer_threads.delete(target)

        logger.info "=== KILLING THREAD #{target} ==="
        target.kill

        sleep 60

        logger.info "=== STARTING NEW CONSUMER THREAD ==="
        consumer_threads << start_consumer(result_queue)
      end
    end

    expect {
      num_messages.times {|i|
        result_queue.deq
        puts "===> Received #{i} messages" if i % 100 == 0
      }
    }.to_not raise_exception
  end

  def start_consumer(result_queue)
    thread = Thread.new do
      begin
        kafka = Kafka.new(
          seed_brokers: KAFKA_BROKERS,
          logger: logger,
          socket_timeout: 20,
          connect_timeout: 20,
        )

        consumer = kafka.consumer(group_id: "fuzz", session_timeout: 10)
        consumer.subscribe(topic)

        consumer.each_message do
          sleep 0.1 # simulate work
          result_queue << :ok
        end
      ensure
        consumer.shutdown
      end
    end

    thread.abort_on_exception = true

    thread
  end
end
