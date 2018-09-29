# frozen_string_literal: true

describe "Idempotent async producer", functional: true do
  let(:producer) { kafka.async_producer(max_retries: 3, delivery_interval: 1, retry_backoff: 1, idempotent: true) }
  after do
    producer.shutdown
  end

  example 'duplication if idempotent is not enabled' do
    producer = kafka.async_producer(max_retries: 3, delivery_interval: 1, retry_backoff: 1, idempotent: false)
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    sleep 2

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
    producer.produce('Hi', topic: topic, partition: 0)
    sleep 2

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    sleep 2

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(3)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
    expect(records[2].value).to eql('Hi')
  end

  example "produces records in normal situation" do
    topic = create_random_topic(num_partitions: 3)

    250.times do |index|
      producer.produce(index.to_s, topic: topic, partition: 0)
      producer.produce(index.to_s, topic: topic, partition: 1)
      producer.produce(index.to_s, topic: topic, partition: 2)
    end
    sleep 2

    kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).each_with_index do |record, index|
      expect(record.value).to eql(index.to_s)
    end
    kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest).each_with_index do |record, index|
      expect(record.value).to eql(index.to_s)
    end
    kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest).each_with_index do |record, index|
      expect(record.value).to eql(index.to_s)
    end
  end

  example "no duplications if brokers are down while writting" do
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    sleep 2

    # Simulate the situation that all brokers fail to write
    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_raise(Errno::ETIMEDOUT)
    producer.produce('Hi', topic: topic, partition: 0)
    sleep 2

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_call_original
    sleep 2

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
  end

  example "no duplication if brokers are down while reading response" do
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    sleep 2

    # Simulate the situation that all brokers fail to read
    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
    producer.produce('Hi', topic: topic, partition: 0)
    sleep 2

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    sleep 2

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
  end

  example "no duplication if one of the brokers are down" do
    topic = create_random_topic(num_partitions: 10)

    10.times do |index|
      producer.produce('Hello', topic: topic, partition: index)
    end
    sleep 2

    # Simulate the situation that only one broker is down
    raised = 1
    begin
      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read) do
        if raised != 0
          raised -= 1
        else
          raise Errno::ETIMEDOUT
        end
      end
    rescue Kafka::DeliveryFailed
    end
    10.times do |index|
      producer.produce('Hi', topic: topic, partition: index)
    end
    sleep 2

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    sleep 2

    10.times do |index|
      records = kafka.fetch_messages(topic: topic, partition: index, offset: :earliest)
      expect(records.length).to eql(2)
      expect(records[0].value).to eql('Hello')
      expect(records[1].value).to eql('Hi')
    end
  end
end
