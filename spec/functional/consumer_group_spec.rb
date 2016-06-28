describe "Consumer API", functional: true do
  let(:num_partitions) { 15 }
  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "consuming messages from the beginning of a topic" do
    sent_messages = 1_000

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

    group_id = "test#{rand(1000)}"

    threads = 2.times.map do |thread_id|
      t = Thread.new do
        received_messages = 0

        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(group_id: group_id)
        consumer.subscribe(topic)

        consumer.each_message do |message|
          break if message.value.nil?
          received_messages += 1
        end

        received_messages
      end

      t.abort_on_exception = true

      t
    end

    received_messages = threads.map(&:value).inject(0, &:+)

    expect(received_messages).to eq sent_messages
  end

  example "consuming messages from the end of a topic" do
    sent_messages = 1_000

    group_id = "test#{rand(1000)}"

    threads = 2.times.map do |thread_id|
      t = Thread.new do
        received_messages = 0

        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(group_id: group_id)
        consumer.subscribe(topic, start_from_beginning: false)

        consumer.each_message do |message|
          break if message.value.nil?
          received_messages += 1
        end

        received_messages
      end

      t.abort_on_exception = true

      t
    end

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

    received_messages = threads.map(&:value).inject(0, &:+)

    expect(received_messages).to eq sent_messages
  end
end
