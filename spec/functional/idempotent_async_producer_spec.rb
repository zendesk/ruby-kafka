# frozen_string_literal: true

describe "Idempotent async producer", functional: true do
  let(:idempotent) { true }
  let(:producer) { kafka.async_producer(idempotent: idempotent) }
  after do
    producer.shutdown
  end

  context 'without idempotence' do
    let(:idempotent) { false }

    example 'duplication if idempotent is not enabled' do
      topic = create_random_topic(num_partitions: 3)

      producer.produce('Hello', topic: topic, partition: 0)
      deliver_and_wait

      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
      producer.produce('Hi', topic: topic, partition: 0)
      deliver_and_wait(expect_failure: true)

      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
      deliver_and_wait

      records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
      expect(records.length).to eql(3)
      expect(records[0].value).to eql('Hello')
      expect(records[1].value).to eql('Hi')
      expect(records[2].value).to eql('Hi')
    end
  end

  example "produces records in normal situation" do
    topic = create_random_topic(num_partitions: 3)

    250.times do |index|
      producer.produce(index.to_s, topic: topic, partition: 0)
      producer.produce(index.to_s, topic: topic, partition: 1)
      producer.produce(index.to_s, topic: topic, partition: 2)
    end
    deliver_and_wait

    3.times do |partition|
      records = fetch_records(topic: topic, partition: partition, count: 250)
      expect(records.map(&:value)).to eq((0...250).map(&:to_s))
    end
  end

  example "no duplications if brokers are down while writting" do
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    deliver_and_wait

    # Simulate the situation that all brokers fail to write
    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_raise(Errno::ETIMEDOUT)
    producer.produce('Hi', topic: topic, partition: 0)
    deliver_and_wait(expect_failure: true)

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:write).and_call_original
    deliver_and_wait

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Hello')
    expect(records[1].value).to eql('Hi')
  end

  example "no duplication if brokers are down while reading response" do
    topic = create_random_topic(num_partitions: 3)

    producer.produce('Hello', topic: topic, partition: 0)
    deliver_and_wait

    # Simulate the situation that all brokers fail to read
    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
    producer.produce('Hi', topic: topic, partition: 0)
    deliver_and_wait(expect_failure: true)

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    deliver_and_wait

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
    deliver_and_wait

    # Simulate the situation that only one broker is down
    failed_socket = nil
    failures = 0
    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_wrap_original do |method, *args|
      failed_socket ||= method.receiver
      if method.receiver == failed_socket
        failures += 1
        raise Errno::ETIMEDOUT
      else
        method.call(*args)
      end
    end
    10.times do |index|
      producer.produce('Hi', topic: topic, partition: index)
    end
    deliver_and_wait
    expect(failures).to be > 0

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original

    10.times do |index|
      records = kafka.fetch_messages(topic: topic, partition: index, offset: :earliest)
      expect(records.length).to eql(2)
      expect(records[0].value).to eql('Hello')
      expect(records[1].value).to eql('Hi')
    end
  end

  # Delivery is queued on another thread. Observe its completion before changing
  # socket faults, so the tests exercise a failed delivery and a real retry.
  def deliver_and_wait(expect_failure: false)
    deliveries = Queue.new
    listener = ->(*args) { deliveries << args.last }

    ActiveSupport::Notifications.subscribed(listener, "deliver_messages.producer.kafka") do
      producer.deliver_messages
      with_retry(retry: 100) { expect(deliveries).not_to be_empty }
      delivery = deliveries.pop
      if expect_failure
        expect(delivery[:exception_object]).to be_a(Kafka::DeliveryFailed)
      else
        expect(delivery[:exception_object]).to be_nil
      end
    end
  end
end
