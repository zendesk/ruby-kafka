# frozen_string_literal: true

require "timeout"

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

  example "subscribing to multiple topics using regex" do
    topic_a = create_random_topic(num_partitions: 1)
    topic_b = create_random_topic(num_partitions: 1)

    messages_a = (1..500).to_a
    messages_b = (501..1000).to_a
    messages = messages_a + messages_b

    begin
      kafka = Kafka.new(kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages_a.each do |i|
        producer.produce(i.to_s, topic: topic_a, partition: 0)
      end

      messages_b.each do |i|
        producer.produce(i.to_s, topic: topic_b, partition: 0)
      end

      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"

    mutex = Mutex.new
    received_messages = []

    consumers = 2.times.map do
      kafka = Kafka.new(kafka_brokers, client_id: "test", logger: logger)
      consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time)
      consumer.subscribe(/#{topic_a}|#{topic_b}/)
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

    expect(received_messages.map(&:value).map(&:to_i).sort).to match_array messages
  end

  example "subscribing to multiple topics using regex and enable refreshing the topic list" do
    topic_a = generate_topic_name
    topic_b = generate_topic_name

    messages_a = (1..500).to_a
    messages_b = (501..1000).to_a
    messages = messages_a + messages_b

    producer = Kafka.new(kafka_brokers, client_id: "test").producer

    messages_a.each { |i| producer.produce(i.to_s, topic: topic_a) }
    producer.deliver_messages

    group_id = "test#{rand(1000)}"

    received_messages = []

    kafka = Kafka.new(kafka_brokers, client_id: "test", logger: logger)
    consumer = kafka.consumer(group_id: group_id, offset_retention_time: offset_retention_time, refresh_topic_interval: 1)
    consumer.subscribe(/#{topic_a}|#{topic_b}/)

    thread = Thread.new do
      consumer.each_message do |message|
        received_messages << message

        if received_messages.count == messages.count
          consumer.stop
        end
      end
    end
    thread.abort_on_exception = true

    sleep 1
    messages_b.each { |i| producer.produce(i.to_s, topic: topic_b) }
    producer.deliver_messages

    thread.join

    expect(received_messages.map(&:value).map(&:to_i).sort).to match_array messages
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

  example "joining a consumer group doesn't reprocess messages" do
    topic = create_random_topic(num_partitions: 4)
    group_id = SecureRandom.uuid

    begin
      # Continuously publish messages in a background thread
      producer_thread = Thread.new do
        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
        producer = kafka.producer
        loop do |i|
          producer.produce("message-#{i}", topic: topic)
          producer.deliver_messages
          sleep 0.1
        end
      end.tap(&:abort_on_exception)

      # A single consumer joins the consumer group and starts consuming
      @consumer_1_messages = []
      consumer_1_thread = Thread.new do
        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
        consumer = kafka.consumer(group_id: group_id)
        consumer.subscribe(topic)
        consumer.each_message do |message|
          @consumer_1_messages << message
        end
      end.tap(&:abort_on_exception)

      # Ensure consumer 1 started processing
      begin
        wait_until(timeout: 20) { @consumer_1_messages.size >= 5 }
      rescue TimeoutError
        fail "consumer #1 didn't consumer 5 messages within 20 seconds"
      end

      # A single consumer joins the consumer group, forcing all consumers to
      # rejoin.
      @consumer_2_messages = []
      consumer_2_thread = Thread.new do
        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
        consumer = kafka.consumer(group_id: group_id)
        consumer.subscribe(topic)
        consumer.each_message do |message|
          @consumer_2_messages << message
        end
      end.tap(&:abort_on_exception)

      # Ensure consumer 2 has joined the group and been assigned partitions
      begin
        wait_until(timeout: 20) { @consumer_2_messages.size >= 5 }
      rescue TimeoutError
        fail "consumer #2 didn't consumer 5 messages within 20 seconds"
      end

      expect(@consumer_1_messages + @consumer_2_messages)
        .to_not contain_duplicate_messages
    ensure
      producer_thread && producer_thread.kill
      consumer_1_thread && consumer_1_thread.kill
      consumer_2_thread && consumer_2_thread.kill
    end
  end

  def wait_until(timeout:)
    Timeout.timeout(timeout) do
      sleep 0.5 until yield
    end
  end

  RSpec::Matchers.define :contain_duplicate_messages do |first|
    match do |actual|
      @dups = actual
        .group_by { |message| [message.partition, message.offset] }
        .select { |offset, messages| messages.size > 1 }

      @dups.size > 0
    end

    failure_message_when_negated do |actual|
      dup_description = @dups.sort.map do |(partition, offset), messages|
        "#{partition}-#{offset} was processed #{messages.size} times"
      end.join(", ")

      "Expected no duplicate messages, but #{dup_description}"
    end
  end
end
