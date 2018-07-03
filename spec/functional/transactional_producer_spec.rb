# frozen_string_literal: true

describe "Transactional producer", functional: true do
  example 'Typical transactional production' do
    producer = kafka.producer(
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

  example 'Multi-topic transaction' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic_1 = create_random_topic(num_partitions: 3)
    topic_2 = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic_1, partition: 0)
    producer.produce('Test 2', topic: topic_1, partition: 1)
    producer.deliver_messages
    producer.produce('Test 3', topic: topic_2, partition: 0)
    producer.produce('Test 4', topic: topic_2, partition: 1)
    producer.produce('Test 5', topic: topic_2, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    records = kafka.fetch_messages(topic: topic_1, partition: 0, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 1')

    records = kafka.fetch_messages(topic: topic_1, partition: 1, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 2')

    records = kafka.fetch_messages(topic: topic_2, partition: 0, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 3')

    records = kafka.fetch_messages(topic: topic_2, partition: 1, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 4')

    records = kafka.fetch_messages(topic: topic_2, partition: 2, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 5')

    producer.shutdown
  end

  example 'Consumer could not read aborted transactional production' do
    producer = kafka.producer(
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
    producer.abort_transaction

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'Multi-topic aborted transactions' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic_1 = create_random_topic(num_partitions: 3)
    topic_2 = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic_1, partition: 0)
    producer.produce('Test 2', topic: topic_1, partition: 1)
    producer.deliver_messages
    producer.produce('Test 3', topic: topic_2, partition: 0)
    producer.produce('Test 4', topic: topic_2, partition: 1)
    producer.produce('Test 5', topic: topic_2, partition: 2)
    producer.deliver_messages
    producer.abort_transaction

    records = kafka.fetch_messages(topic: topic_1, partition: 0, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic_1, partition: 1, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic_2, partition: 0, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic_2, partition: 1, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic_2, partition: 2, offset: :earliest)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'Fenced-out producer' do
    transactional_id = SecureRandom.uuid
    topic = create_random_topic(num_partitions: 3)

    producer_1 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )

    producer_1.init_transactions
    producer_1.begin_transaction
    producer_1.produce('Test 1', topic: topic, partition: 0)
    producer_1.produce('Test 2', topic: topic, partition: 1)
    producer_1.produce('Test 3', topic: topic, partition: 2)
    producer_1.deliver_messages
    producer_1.commit_transaction

    producer_2 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )
    producer_2.init_transactions
    producer_2.begin_transaction
    producer_2.produce('Test 4', topic: topic, partition: 0)
    producer_2.produce('Test 5', topic: topic, partition: 1)
    producer_2.deliver_messages
    producer_2.commit_transaction

    producer_1.begin_transaction
    producer_1.produce('Test 6', topic: topic, partition: 0)
    expect do
      producer_1.deliver_messages
    end.to raise_error(Kafka::InvalidProducerEpochError)

    producer_1.shutdown
    producer_2.shutdown
  end

  example 'Concurrent transaction' do
    transactional_id = SecureRandom.uuid
    topic = create_random_topic(num_partitions: 3)

    producer_1 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )

    producer_1.init_transactions
    producer_1.begin_transaction
    producer_1.produce('Test 1', topic: topic, partition: 0)
    producer_1.produce('Test 2', topic: topic, partition: 1)
    producer_1.produce('Test 3', topic: topic, partition: 2)
    producer_1.deliver_messages

    producer_2 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )
    expect do
      producer_2.init_transactions
    end.to raise_error(Kafka::ConcurrentTransactionError)

    begin
      producer_1.shutdown
      producer_2.shutdown
    rescue; end
  end

  example 'with_transaction syntax' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.with_transaction do
      producer.produce('Test 1', topic: topic, partition: 0)
      producer.produce('Test 2', topic: topic, partition: 1)
      producer.deliver_messages
      producer.produce('Test 3', topic: topic, partition: 2)
      producer.deliver_messages
    end

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

  example 'with_transaction block raises error' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    expect do
      producer.with_transaction do
        producer.produce('Test 1', topic: topic, partition: 0)
        producer.produce('Test 2', topic: topic, partition: 1)
        producer.produce('Test 3', topic: topic, partition: 2)
        producer.deliver_messages
        raise 'Something went wrong'
      end
    end.to raise_error(/something went wrong/i)

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'with_transaction block actively aborts the transaction' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    expect do
      producer.with_transaction do
        producer.produce('Test 1', topic: topic, partition: 0)
        producer.produce('Test 2', topic: topic, partition: 1)
        producer.produce('Test 3', topic: topic, partition: 2)
        producer.deliver_messages
        raise Kafka::Producer::AbortTransaction
      end
    end.not_to raise_error

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'Transaction is idempotent by default' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic, partition: 0)
    producer.deliver_messages

    producer.produce('Test 2', topic: topic, partition: 0)
    begin
      allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_raise(Errno::ETIMEDOUT)
      producer.deliver_messages
    rescue
    end

    allow_any_instance_of(Kafka::SocketWithTimeout).to receive(:read).and_call_original
    producer.deliver_messages
    producer.commit_transaction

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(2)
    expect(records[0].value).to eql('Test 1')
    expect(records[1].value).to eql('Test 2')

    producer.shutdown
  end

  # Expensive tests to run. The transactional timeout is a myth. Kafka doesn't
  # handle short timeout well. It works perfectly for longer timeout (over 1
  # minute).
  example 'Timeout transaction' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid,
      transactional_timeout: 5
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    expect do
      120.times do |index|
        producer.produce("Test #{index}", topic: topic, partition: 0)
        producer.deliver_messages
        sleep 1
      end
    end.to raise_error(Kafka::InvalidProducerEpochError)

    expect do
      producer.commit_transaction
    end.to raise_error(Kafka::InvalidProducerEpochError)

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest, max_wait_time: 1)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'Client excludes aborted messages' do
    topic = create_random_topic(num_partitions: 3)

    producer_1 = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )
    producer_2 = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    producer_1.init_transactions
    producer_2.init_transactions

    producer_1.begin_transaction
    producer_1.produce('Test 1', topic: topic, partition: 0)
    producer_1.produce('Test 2', topic: topic, partition: 1)
    producer_1.deliver_messages

    producer_2.begin_transaction
    producer_2.produce('Test 3', topic: topic, partition: 0)
    producer_2.produce('Test 4', topic: topic, partition: 1)
    producer_2.produce('Test 5', topic: topic, partition: 2)
    producer_2.deliver_messages

    producer_1.commit_transaction
    producer_2.commit_transaction

    producer_1.begin_transaction
    producer_1.produce('Test 6', topic: topic, partition: 0)
    producer_1.produce('Test 7', topic: topic, partition: 1)
    producer_1.deliver_messages
    producer_1.commit_transaction

    producer_2.begin_transaction
    producer_2.produce('Test 8', topic: topic, partition: 0)
    producer_2.produce('Test 9', topic: topic, partition: 1)
    producer_2.produce('Test 10', topic: topic, partition: 2)
    producer_2.deliver_messages
    producer_2.abort_transaction

    producer_1.begin_transaction
    producer_1.produce('Test 11', topic: topic, partition: 0)
    producer_1.produce('Test 12', topic: topic, partition: 1)
    producer_1.deliver_messages
    producer_1.commit_transaction

    sleep 1

    records = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest)
    expect(records.length).to eql(4)
    expect(records.map(&:value)).to match_array(['Test 1', 'Test 3', 'Test 6', 'Test 11'])

    records = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest)
    expect(records.length).to eql(4)
    expect(records.map(&:value)).to match_array(['Test 2', 'Test 4', 'Test 7', 'Test 12'])

    records = kafka.fetch_messages(topic: topic, partition: 2, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records.map(&:value)).to eql(['Test 5'])

    producer_1.shutdown
    producer_2.shutdown
  end
end
