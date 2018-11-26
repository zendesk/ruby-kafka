# frozen_string_literal: true

describe "Producer API", functional: true do
  let(:producer) { kafka.producer(max_retries: 3, retry_backoff: 1) }

  after do
    producer.shutdown
  end

  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "setting a create_time value" do
    timestamp = Time.now

    producer.produce("hello", topic: topic, partition: 0, create_time: timestamp)
    producer.deliver_messages

    message = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last

    expect(message.create_time.to_i).to eq timestamp.to_i
  end

  example "writing messages using the buffered producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: topic, partition: 0)
    producer.produce(value2, key: "y", topic: topic, partition: 1)

    producer.deliver_messages

    message1 = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "having the producer assign partitions based on partition keys" do
    producer.produce("hello1", key: "x", topic: topic, partition_key: "xk")
    producer.produce("hello2", key: "y", topic: topic, partition_key: "yk")

    producer.deliver_messages
  end

  example "having the producer assign partitions based on message keys" do
    producer.produce("hello1", key: "x", topic: topic)
    producer.produce("hello2", key: "y", topic: topic)

    producer.deliver_messages
  end

  example "omitting message keys entirely" do
    producer.produce("hello1", topic: topic)
    producer.produce("hello2", topic: topic)

    producer.deliver_messages
  end

  example "writing to a an explicit partition of a topic that doesn't yet exist" do
    topic = "topic#{SecureRandom.uuid}"
    create_topic(topic)

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic, partition: 0)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example "writing to a an unspecified partition of a topic that doesn't yet exist" do
    topic = "topic#{SecureRandom.uuid}"
    create_topic(topic)

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example 'support record headers' do
    topic = "topic#{SecureRandom.uuid}"
    create_topic(topic)

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce(
      "hello", topic: topic,
      headers: { hello: 'World', 'greeting' => 'is great', bye: 1, love: nil }
    )
    producer.produce(
      "hello2", topic: topic,
      headers: { 'other' => 'headers' }
    )
    producer.produce("hello3", topic: topic, headers: {})
    producer.produce("hello4", topic: topic)

    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages[0].value).to eq "hello"
    expect(messages[0].headers).to eql(
      'hello' => 'World',
      'greeting' => 'is great',
      'bye' => '1',
      'love' => ''
    )

    expect(messages[1].value).to eq "hello2"
    expect(messages[1].headers).to eql(
      'other' => 'headers'
    )

    expect(messages[2].value).to eq "hello3"
    expect(messages[2].headers).to eql({})

    expect(messages[3].value).to eq "hello4"
    expect(messages[3].headers).to eql({})
  end

  example 'failing a message with metadata' do
    cluster = instance_double(Kafka::Cluster)
    expect(cluster).to receive(:add_target_topics)
    expect(cluster).to receive(:refresh_metadata_if_necessary!).and_raise(Kafka::ConnectionError)

    instrumenter = Kafka::Instrumenter.new(client_id: 'abc')

    transman = instance_double(Kafka::TransactionManager)
    allow(transman).to receive(:transactional?).and_return(false)

    producer = Kafka::Producer.new(
      cluster: cluster,
      transaction_manager: transman,
      logger: double('logger'),
      instrumenter: instrumenter,
      compressor: double('compressor'),
      ack_timeout: 5,
      required_acks: 1,
      max_retries: 0,
      retry_backoff: 0,
      max_buffer_size: 1000,
      max_buffer_bytesize: 1024
    )

    callback = lambda do |*args|
      event = ActiveSupport::Notifications::Event.new(*args)
      expect(event.payload.fetch(:exception_object)).to be
      messages = event.payload[:exception_object].failed_messages
      expect(messages.size).to eq(2)
      expect(messages[0].metadata[:publisher_name]).to eq('MyPublisher')
      expect(messages[0].value).to eq('MyValue')
      expect(messages[1].metadata[:publisher_name]).to eq('MyPublisher2')
      expect(messages[1].value).to eq('MyValue2')
    end

    ActiveSupport::Notifications.subscribed(callback, 'deliver_messages.producer.kafka') do
      expect {
        producer.produce('MyValue', topic: 'my-topic', metadata: { publisher_name: 'MyPublisher' })
        producer.produce('MyValue2', topic: 'my-topic', metadata: { publisher_name: 'MyPublisher2' })
        producer.deliver_messages
      }.to raise_error(Kafka::DeliveryFailed)
    end
  end
end
