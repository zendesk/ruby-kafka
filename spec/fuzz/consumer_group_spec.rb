describe "Consumer groups", fuzz: true do
  let(:logger) { Logger.new(LOG) }
  let(:num_messages) { 100_000 }
  let(:num_partitions) { 30 }
  let(:num_consumers) { 10 }
  let(:topic) { "fuzz-consumer-group" }
  let(:messages) { Set.new((1..num_messages).to_a) }

  before do
    require "test_cluster"

    logger.level = Logger::INFO

    KAFKA_CLUSTER.create_topic(topic, num_partitions: num_partitions, num_replicas: 1)

    kafka = Kafka.new(seed_brokers: KAFKA_BROKERS, logger: logger)
    producer = kafka.producer(max_buffer_size: 5000)

    messages.each do |i|
      producer.produce(i.to_s, topic: topic, partition: i % num_partitions)
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

    missing_messages = messages.dup
    duplicate_messages = Set.new

    expect {
      until missing_messages.empty?
        message = result_queue.deq

        if missing_messages.delete?(message)
          size = num_messages - missing_messages.size
          puts "===> Received #{size} messages" if size % 100 == 0
        else
          duplicate_messages.add(message)
        end
      end
    }.to_not raise_exception

    expect(missing_messages).to eq Set.new

    puts "#{duplicate_messages.size} duplicate messages!"
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

        consumer.each_message do |message|
          sleep 0.1 # simulate work
          result_queue << Integer(message.value)
        end
      ensure
        consumer.shutdown
      end
    end

    thread.abort_on_exception = true

    thread
  end
end
