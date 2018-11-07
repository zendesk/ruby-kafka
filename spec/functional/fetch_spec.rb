# frozen_string_literal: true

describe "Fetch API", functional: true do
  example "fetching from a non-existing topic when auto-create is enabled" do
    topic = "rand#{SecureRandom.uuid}"
    attempt = 1
    messages = nil

    begin
      messages = kafka.fetch_messages(
        topic: topic,
        partition: 0,
        offset: 0,
        max_wait_time: 0.1
      )
    rescue Kafka::LeaderNotAvailable, Kafka::NotLeaderForPartition
      if attempt < 10
        attempt += 1
        sleep 0.1
        retry
      else
        raise "timed out"
      end
    end

    expect(messages).to eq []
  end

  example "Number of fetching requests should be small" do
    kafka = ::Kafka::Client.new(
      seed_brokers: ['localhost:9092']
    )
    total_messages = 1000
    message_size = 50

    # Create test data
    producer = kafka.producer
    topic = "topic-#{SecureRandom.uuid}"
    kafka.create_topic(topic, num_partitions: 3)

    total_messages.times do |index|
      producer.produce('a' * message_size, topic: topic)
    end
    producer.deliver_messages

    batch_count = 0
    ActiveSupport::Notifications.subscribe 'fetch_batch.consumer.kafka' do |*args|
      batch_count += 1
    end
    # Test consuming
    consumer = kafka.consumer(group_id: SecureRandom.uuid)
    consumer.subscribe(topic)
    message_count = 0
    consumer.each_message do
      message_count += 1
      break if message_count == total_messages
    end

    expect(batch_count).to be <= 4
  end
end
