# frozen_string_literal: true

describe "Idempotent producer API", functional: true do
  let(:producer) { kafka.producer(max_retries: 3, retry_backoff: 1, idempotent: true) }

  after do
    producer.shutdown
  end

  example 'duplication if idempotent is not enabled' do
    producer = kafka.producer(max_retries: 3, retry_backoff: 1, idempotent: false)
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    producer.deliver_messages

    producer.produce('Hi', topic: topic, partition: 0)
    begin
      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
      producer.deliver_messages
    rescue
    end

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    producer.deliver_messages

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
      producer.deliver_messages if index % 50 == 0
    end
    producer.deliver_messages

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
    producer.deliver_messages

    producer.produce('Hi', topic: topic, partition: 0)
    # Simulate the situation that all brokers fail to write
    begin
      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_raise(Errno::ETIMEDOUT)
      producer.deliver_messages
    rescue
    end

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_call_original
    producer.deliver_messages

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
  end

  example "no duplication if brokers are down while reading response" do
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    producer.deliver_messages

    producer.produce('Hi', topic: topic, partition: 0)

    # Simulate the situation that all brokers fail to read
    begin
      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
      producer.deliver_messages
    rescue
    end

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    producer.deliver_messages

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
  end

  example "no duplication if one of the brokers are down" do
    topic = create_random_topic(num_partitions: 100)

    100.times do |index|
      producer.produce('Hello', topic: topic, partition: index)
    end

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
      producer.deliver_messages
    rescue
    end

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    producer.deliver_messages

    100.times do |index|
      records = kafka.fetch_messages(topic: topic, partition: index, offset: :earliest)
      expect(records.length).to eql(1)
      expect(records.first.value).to eql('Hello')
    end
  end
end
