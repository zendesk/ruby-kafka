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

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Test 1')
    expect(records[1].value).to eql('Test 3')

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Test 2')
    expect(records[1].value).to eql('Test 4')

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 5')

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

    sleep 1

    producer.begin_transaction
    producer.produce('Test 3', topic: topic, partition: 0)
    producer.produce('Test 4', topic: topic, partition: 1)
    producer.produce('Test 5', topic: topic, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Test 1')
    expect(records[1].value).to eql('Test 3')

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Test 2')
    expect(records[1].value).to eql('Test 4')

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 5')
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

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    producer.commit_transaction

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 1')

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 2')

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 3')

    producer.shutdown
  end
end
