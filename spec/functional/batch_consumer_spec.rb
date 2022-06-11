# frozen_string_literal: true

describe "Batch Consumer API", functional: true do
  example "consuming messages using the batch API" do
    num_partitions = 15
    message_count = 1_000
    messages = (1...message_count).to_set
    message_queue = Queue.new
    offset_retention_time = 30

    topic = create_random_topic(num_partitions: 15)

    Thread.new do
      kafka = Kafka.new(kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages.each do |i|
        producer.produce(i.to_s, topic: topic, partition_key: i.to_s)
      end

      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"

    threads = 2.times.map do |thread_id|
      t = Thread.new do
        kafka = Kafka.new(kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time)
        consumer.subscribe(topic)

        consumer.each_batch do |batch|
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
      message = begin
        Timeout.timeout(10) { message_queue.pop }
      rescue Timeout::Error
        $stderr.puts "Timeout::Error: No message was added within the time-out period"
        break
      end

      if received_messages.include?(message)
        duplicates.add(message)
      else
        received_messages.add(message)
      end

      break if received_messages.size == messages.size
    end

    expect(received_messages).to eq messages
    expect(duplicates).to eq Set.new
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
    consumer.each_batch do |batch|
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
