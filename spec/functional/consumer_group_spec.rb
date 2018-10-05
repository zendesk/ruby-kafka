# frozen_string_literal: true

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

  example "consumers process all messages in-order and non-duplicated" do
    topic = create_random_topic(num_partitions: 2)
    message_count = 500
    messages_set_1 = (1..500).to_a
    messages_set_2 = (501..1000).to_a

    begin
      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
      producer = kafka.producer
      messages_set_1.each do |i|
        producer.produce(i.to_s, topic: topic, partition: 0)
      end
      messages_set_2.each do |i|
        producer.produce(i.to_s, topic: topic, partition: 1)
      end
      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"
    received_messages = {}

    consumers = 2.times.map do
      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
      consumer = kafka.consumer(group_id: group_id)
      consumer.subscribe(topic)
      consumer
    end

    threads = consumers.map do |consumer|
      t = Thread.new do
        received_messages[Thread.current] = []
        consumer.each_message do |message|
          received_messages[Thread.current] << message

          if received_messages[Thread.current].count == message_count
            consumer.stop
          end
        end
      end
      t.abort_on_exception = true
      t
    end

    threads.each(&:join)

    received_messages.each do |_thread, messages|
      values = messages.map(&:value).map(&:to_i)
      if messages.first.partition == 0
        expect(values).to eql(messages_set_1)
      else
        expect(values).to eql(messages_set_2)
      end
    end
  end

  example 'support record headers' do
    topic = create_random_topic(num_partitions: 1)
    kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
    producer = kafka.producer
    producer.produce(
      'hello', topic: topic, headers: { 'TracingID' => 'a1', 'SpanID' => 'b2' }
    )
    producer.produce(
      'hello2', topic: topic, headers: { 'TracingID' => 'c3', 'SpanID' => 'd4' }
    )
    producer.deliver_messages
    consumer = kafka.consumer(group_id: SecureRandom.uuid)
    consumer.subscribe(topic)

    headers = []
    consumer.each_message do |message|
      headers << message.headers
      break if headers.length == 2
    end

    expect(headers).to eql(
      [
        { 'TracingID' => 'a1', 'SpanID' => 'b2' },
        { 'TracingID' => 'c3', 'SpanID' => 'd4' }
      ]
    )
  end

  example 'consumer ignored consumed records of a record batch' do
    topic = create_random_topic(num_partitions: 1)
    kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
    producer = kafka.producer
    group_id = SecureRandom.uuid

    (1..4).each do |index|
      producer.produce(index.to_s, topic: topic)
    end
    producer.deliver_messages

    count = 0
    data = []

    consumer = kafka.consumer(group_id: group_id)
    consumer.subscribe(topic)

    consumer.each_message(automatically_mark_as_processed: false) do |message|
      count += 1
      consumer.mark_message_as_processed(message)
      consumer.commit_offsets
      data << message.value
      break if count == 3
    end

    consumer_2 = kafka.consumer(group_id: group_id)
    consumer_2.subscribe(topic)

    consumer_2.each_message(automatically_mark_as_processed: false) do |message|
      consumer_2.mark_message_as_processed(message)
      consumer_2.commit_offsets
      data << message.value
      break
    end

    expect(data).to eql(%w(1 2 3 4))
  end

  example 'pause permanently a partition' do
    topic = create_random_topic(num_partitions: 3)

    kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
    producer = kafka.producer
    producer.produce('hello', topic: topic, partition: 0)
    producer.produce('hello2', topic: topic, partition: 0)
    producer.produce('hi', topic: topic, partition: 1)
    producer.produce('bye', topic: topic, partition: 2)

    producer.deliver_messages

    consumer = kafka.consumer(group_id: SecureRandom.uuid)
    consumer.subscribe(topic)

    records = []
    t = Thread.new do
      consumer.each_message do |message|
        if message.partition == 0
          consumer.pause(topic, 0)
        end
        records << message.value
      end
    end
    t.abort_on_exception = true

    sleep 5
    t.kill

    expect(records).to match_array(
      ['hello', 'hi', 'bye']
    )
  end

  example 'pause with timeout' do
    topic = create_random_topic(num_partitions: 3)

    kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
    producer = kafka.producer
    producer.produce('hello', topic: topic, partition: 0)
    producer.produce('hello2', topic: topic, partition: 0)
    producer.produce('hi', topic: topic, partition: 1)
    producer.produce('bye', topic: topic, partition: 2)

    producer.deliver_messages

    consumer = kafka.consumer(group_id: SecureRandom.uuid)
    consumer.subscribe(topic)

    records = []
    t = Thread.new do
      consumer.each_message do |message|
        if message.partition == 0
          consumer.pause(topic, 0, timeout: 4)
        end
        records << message.value
      end
    end
    t.abort_on_exception = true

    sleep 3
    expect(records).to match_array(
      ['hello', 'hi', 'bye']
    )
    sleep 10
    expect(records).to match_array(
      ['hello', 'hello2', 'hi', 'bye']
    )
  end

  example 'pause then resume' do
    topic = create_random_topic(num_partitions: 3)

    kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
    producer = kafka.producer
    producer.produce('hello', topic: topic, partition: 0)
    producer.produce('hello2', topic: topic, partition: 0)
    producer.produce('hi', topic: topic, partition: 1)
    producer.produce('bye', topic: topic, partition: 2)

    producer.deliver_messages

    consumer = kafka.consumer(group_id: SecureRandom.uuid)
    consumer.subscribe(topic)

    records = []
    t = Thread.new do
      consumer.pause(topic, 0)
      consumer.each_message do |message|
        records << message.value
      end
    end
    t.abort_on_exception = true

    sleep 3
    expect(records).to match_array(
      ['hi', 'bye']
    )

    consumer.resume(topic, 0)
    sleep 10
    expect(records).to match_array(
      ['hello', 'hello2', 'hi', 'bye']
    )

    t.kill
  end
end
