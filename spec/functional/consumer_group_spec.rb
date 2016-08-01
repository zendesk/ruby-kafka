describe "Consumer API", functional: true do
  let(:num_partitions) { 15 }
  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "consuming messages from the beginning of a topic" do
    messages = (1..1000).to_a

    Thread.new do
      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages.each do |i|
        producer.produce(i.to_s, topic: topic, partition_key: i.to_s)

        if i % 100 == 0
          producer.deliver_messages 
          sleep 1
        end
      end

      (0...num_partitions).each do |i|
        # Send a tombstone to each partition.
        producer.produce(nil, topic: topic, partition: i)
      end

      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"

    threads = 2.times.map do |thread_id|
      t = Thread.new do
        received_messages = []

        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(group_id: group_id)
        consumer.subscribe(topic)

        consumer.each_message do |message|
          if message.value.nil?
            consumer.stop
          else
            received_messages << Integer(message.value)
          end
        end

        received_messages
      end

      t.abort_on_exception = true

      t
    end

    received_messages = threads.map(&:value).flatten

    expect(received_messages.sort).to match_array messages
  end

  example "consuming messages from the end of a topic" do
    sent_messages = 1_000

    num_partitions = 1
    topic = create_random_topic(num_partitions: num_partitions)
    group_id = "test#{rand(1000)}"

    consumer_thread = Thread.new do
      received_messages = 0

      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
      consumer = kafka.consumer(group_id: group_id)
      consumer.subscribe(topic, start_from_beginning: false)

      consumer.each_message do |message|
        if message.value.nil?
          consumer.stop
        else
          received_messages += 1
        end
      end

      received_messages
    end

    consumer_thread.abort_on_exception = true

    sleep 30

    Thread.new do
      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
      producer = kafka.producer

      1.upto(sent_messages) do |i|
        producer.produce("hello", topic: topic, partition_key: i.to_s)

        if i % 100 == 0
          producer.deliver_messages 
          sleep 1
        end
      end

      (0...num_partitions).each do |i|
        # Send a tombstone to each partition.
        producer.produce(nil, topic: topic, partition: i)
      end

      producer.deliver_messages
    end

    received_messages = consumer_thread.value

    expect(received_messages).to eq sent_messages
  end

  example "stopping and restarting a consumer group" do
    topic = create_random_topic(num_partitions: 1)
    num_messages = 10
    processed_messages = 0

    producer = kafka.producer
    (1..num_messages).each {|i| producer.produce(i.to_s, topic: topic) }
    producer.deliver_messages

    group_id = "test#{rand(1000)}"

    consumer = kafka.consumer(group_id: group_id)
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
