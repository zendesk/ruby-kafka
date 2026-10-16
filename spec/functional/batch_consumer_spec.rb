# frozen_string_literal: true

describe "Batch Consumer API", functional: true do
  example "consuming messages using the batch API" do
    num_partitions = 15
    message_count = 1_000
    messages = (1...message_count).to_set
    message_queue = Queue.new
    offset_retention_time = 30

    topic = create_random_topic(num_partitions: 15)

    producer_thread = Thread.new do
      kafka = Kafka.new(kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages.each do |i|
        producer.produce(i.to_s, topic: topic, partition_key: i.to_s)
      end

      producer.deliver_messages
    ensure
      producer && producer.shutdown
      kafka && kafka.close
    end
    producer_thread.abort_on_exception = true

    group_id = SecureRandom.uuid

    consumers = 2.times.map do
      consumer = create_consumer(kafka, group_members: 2, group_id: group_id, offset_retention_time: offset_retention_time)
      consumer.subscribe(topic)
      consumer
    end

    threads = consumers.map do |consumer|
      t = Thread.new do
        consumer.each_batch(max_wait_time: 0.1) do |batch|
          batch.messages.each do |message|
            message_queue << Integer(message.value)
          end
        end
      end

      t.abort_on_exception = true

      t
    end

    received_messages = Set.new
    duplicates = Set.new

    loop do
      message = message_queue.pop

      if received_messages.include?(message)
        duplicates.add(message)
      else
        received_messages.add(message)
      end

      break if received_messages.size == messages.size
    end

    expect(received_messages).to eq messages
    expect(duplicates).to eq Set.new
  ensure
    consumers && consumers.each(&:stop)
    threads && threads.each(&:join)
    producer_thread && producer_thread.join
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
    consumer = create_consumer(kafka, group_id: SecureRandom.uuid)
    consumer.subscribe(topic)

    headers = []
    consumer.each_batch(max_wait_time: 0.1) do |batch|
      batch.messages.each do |message|
        headers << message.headers
      end
      break
    end

    expect(headers).to eql(
      [
        { 'TracingID' => 'a1', 'SpanID' => 'b2' },
        { 'TracingID' => 'c3', 'SpanID' => 'd4' }
      ]
    )
  end
end
