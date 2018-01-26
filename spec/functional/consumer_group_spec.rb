describe "Consumer API", functional: true do
  let(:offset_retention_time) { 30 }

  example "consuming messages from the beginning of a topic" do
    topic = create_random_topic(num_partitions: 1)
    messages = (1..1000).to_a

    begin
      kafka = Kafka.new(kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages.each do |i|
        producer.produce(i.to_s, topic: topic, partition: 0)
      end

      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"

    mutex = Mutex.new
    received_messages = []

    consumers = 2.times.map do
      kafka = Kafka.new(kafka_brokers, client_id: "test", logger: logger)
      consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time)
      consumer.subscribe(topic)
      consumer
    end

    threads = consumers.map do |consumer|
      t = Thread.new do
        consumer.each_message do |message|
          mutex.synchronize do
            received_messages << message

            if received_messages.count == messages.count
              consumers.each(&:stop)
            end
          end
        end
      end

      t.abort_on_exception = true

      t
    end

    threads.each(&:join)

    expect(received_messages.map(&:value).map(&:to_i)).to match_array messages
  end

  example "consuming messages from a topic that's being written to" do
    num_partitions = 3
    topic = create_random_topic(num_partitions: num_partitions)
    messages = (1..100).to_a

    mutex = Mutex.new
    var = ConditionVariable.new

    Thread.new do
      kafka = Kafka.new(kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages.each do |i|
        producer.produce(i.to_s, topic: topic, partition: i % 3)

        if i % 100 == 0
          producer.deliver_messages

          mutex.synchronize do
            var.wait(mutex)
          end
        end
      end

      (0...num_partitions).each do |i|
        # Send a tombstone to each partition.
        producer.produce(nil, topic: topic, partition: i)
      end

      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"
    received_messages = []

    threads = 2.times.map do |thread_id|
      t = Thread.new do
        kafka = Kafka.new(kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time)
        consumer.subscribe(topic)

        consumer.each_message do |message|
          if message.value.nil?
            consumer.stop
          else
            mutex.synchronize do
              received_messages << Integer(message.value)
              var.signal
            end
          end
        end
      end

      t.abort_on_exception = true

      t
    end

    threads.each(&:join)

    expect(received_messages).to match_array messages
  end

  example "stopping and restarting a consumer group" do
    topic = create_random_topic(num_partitions: 1)
    num_messages = 10
    processed_messages = 0

    producer = kafka.producer
    (1..num_messages).each {|i| producer.produce(i.to_s, topic: topic) }
    producer.deliver_messages

    group_id = "test#{rand(1000)}"

    consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time)
    consumer.subscribe(topic, start_from_beginning: true)

    consumer.each_message do |message|
      processed_messages += 1
      consumer.stop if Integer(message.value) == num_messages
    end

    expect(processed_messages).to eq num_messages

    (1..num_messages).each {|i| producer.produce(i.to_s, topic: topic) }
    producer.deliver_messages

    consumer = kafka.consumer(group_id: group_id)
    consumer.subscribe(topic, start_from_beginning: true)

    consumer.each_message do |message|
      processed_messages += 1
      consumer.stop if Integer(message.value) == num_messages
    end

    expect(processed_messages).to eq(num_messages * 2)
  end
end
