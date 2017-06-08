describe "Producer API", functional: true do
  let(:producer) { kafka.async_producer(max_retries: 1, retry_backoff: 0) }
  let(:topic) { create_random_topic(num_partitions: 3) }

  after do
    producer.shutdown
  end

  example "writing messages using async producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: topic, partition: 0)
    producer.produce(value2, key: "y", topic: topic, partition: 1)

    producer.deliver_messages

    # Wait for everything to be delivered.
    producer.shutdown

    message1 = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "automatically delivering messages with a fixed time interval" do
    producer = kafka.async_producer(delivery_interval: 0.1)

    value = rand(10_000).to_s
    producer.produce(value, topic: topic, partition: 0)

    sleep 0.2

    messages = kafka.fetch_messages(
      topic: topic,
      partition: 0,
      offset: 0,
      max_wait_time: 0.1,
    )

    expect(messages.last.value).to eq value

    producer.shutdown
  end

  example "automatically delivering messages when a buffer threshold is reached" do
    producer = kafka.async_producer(delivery_threshold: 5)

    values = 5.times.map { rand(10_000).to_s }

    values.each do |value|
      producer.produce(value, topic: topic, partition: 0)
    end

    sleep 0.2

    messages = kafka.fetch_messages(
      topic: topic,
      partition: 0,
      offset: 0,
      max_wait_time: 0,
    )

    expect(messages.last(5).map(&:value)).to eq values

    producer.shutdown
  end

  example "ssl exceptions are hidden on async producers" do
    producer = kafka.async_producer(delivery_threshold: 1)

    #expect(producer.instance_variable_get(:@worker)).to receive(:run).and_raise(OpenSSL::SSL::SSLError)

    #expect(Kafka::SSLSocketWithTimeout.any_instance).to receive(:initialize).and_raise(OpenSSL::SSL::SSLError)
    #expect(Kafka::SocketWithTimeout.any_instance).to receive(:initialize).and_raise(OpenSSL::SSL::SSLError)
    #expect(Kafka::Connection.any_instance).to receive(:send_request).and_raise(OpenSSL::SSL::SSLError)
    #expect(Kafka::Producer.any_instance).to receive(:deliver_messages_with_retries).and_raise(OpenSSL::SSL::SSLError)
    #expect(producer).to receive(:deliver_messages).and_raise(OpenSSL::SSL::SSLError)
    #expect(Kafka::Producer.any_instance).to receive(:deliver_messages).and_raise(OpenSSL::SSL::SSLError)
    #expect(producer.instance_variable_get(:@worker)).to receive(:deliver_messages).and_raise(OpenSSL::SSL::SSLError)
    expect(producer.instance_variable_get(:@worker).instance_variable_get(:@producer)).to receive(:deliver_messages).and_raise(OpenSSL::SSL::SSLError)
    #expect(producer).to receive(:deliver_messages).and_raise(OpenSSL::SSL::SSLError)

    #expect {
      producer.produce("value", topic: topic, partition: 0) #}.to raise_error(OpenSSL::SSL::SSLError)
    sleep 0.2
=begin

    messages = kafka.fetch_messages(
      topic: topic,
      partition: 0,
      offset: 0,
      max_wait_time: 0,
    )

    expect(messages.size).to eq 0

    producer.shutdown
=end
  end

  example "non existent topics throw " do
    producer = kafka.async_producer(delivery_threshold: 1)

    producer.produce("value", topic: "non_existent_topic", partition: 0)

    sleep 0.2

    messages = kafka.fetch_messages(
      topic: topic,
      partition: 0,
      offset: 0,
      max_wait_time: 0,
    )

    expect(messages.size).to eq 0

    producer.shutdown
  end
end
