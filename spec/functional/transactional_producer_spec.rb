# frozen_string_literal: true

describe "Transactional producer", functional: true do
  example 'Typical transactional production' do
    producer = kafka.producer(
      max_retries: 3,
      retry_backoff: 1,
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic, partition: 0)
    producer.produce('Test 2', topic: topic, partition: 1)
    producer.deliver_messages
    producer.produce('Test 3', topic: topic, partition: 0)
    producer.produce('Test 4', topic: topic, partition: 1)
    producer.produce('Test 5', topic: topic, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    expect_topic_messages(topic, ['Test 1', 'Test 2', 'Test 3', 'Test 4', 'Test 5'])

    producer.shutdown
  end

  example 'Multiple transactional production' do
    producer = kafka.producer(
      max_retries: 3,
      retry_backoff: 1,
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic, partition: 0)
    producer.produce('Test 2', topic: topic, partition: 1)
    producer.deliver_messages
    producer.commit_transaction

    producer.begin_transaction
    producer.produce('Test 3', topic: topic, partition: 0)
    producer.produce('Test 4', topic: topic, partition: 1)
    producer.produce('Test 5', topic: topic, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    expect_topic_messages(topic, ['Test 1', 'Test 2', 'Test 3', 'Test 4', 'Test 5'])
    producer.shutdown
  end

  example 'Consumer could not read not-completed transactional production' do
    producer = kafka.producer(
      max_retries: 3,
      retry_backoff: 1,
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic, partition: 0)
    producer.produce('Test 2', topic: topic, partition: 1)
    producer.produce('Test 3', topic: topic, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    producer.begin_transaction
    producer.produce('Test 4', topic: topic, partition: 0)
    producer.produce('Test 5', topic: topic, partition: 1)
    producer.deliver_messages

    expect_topic_messages(topic, ['Test 1', 'Test 2', 'Test 3'])
    producer.commit_transaction
    expect_topic_messages(topic, ['Test 1', 'Test 2', 'Test 3', 'Test 4', 'Test 5'])
    producer.shutdown
  end
end

def expect_topic_messages(topic, expected_messages)
  consumer = kafka.consumer(group_id: SecureRandom.uuid)
  consumer.subscribe(topic)
  messages = []
  consumer.each_message do |message|
    messages << message.value
    break if messages.length == expected_messages.length
  end
  expect(messages).to match_array(expected_messages)
end
