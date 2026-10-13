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

    topic1 = create_random_topic(num_partitions: 3)
    topic2 = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic1, partition: 0)
    producer.produce('Test 2', topic: topic1, partition: 1)
    producer.deliver_messages
    producer.produce('Test 3', topic: topic2, partition: 0)
    producer.produce('Test 4', topic: topic2, partition: 1)
    producer.produce('Test 5', topic: topic2, partition: 2)
    producer.deliver_messages
    producer.commit_transaction

    records = kafka.fetch_messages(topic: topic1, partition: 0, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 1')

    records = kafka.fetch_messages(topic: topic1, partition: 1, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 2')

    records = kafka.fetch_messages(topic: topic2, partition: 0, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 3')

    records = kafka.fetch_messages(topic: topic2, partition: 1, offset: :earliest)
    expect(records.length).to eql(1)
    expect(records[0].value).to eql('Test 4')

    records = kafka.fetch_messages(topic: topic2, partition: 2, offset: :earliest)
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

    topic1 = create_random_topic(num_partitions: 3)
    topic2 = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.begin_transaction
    producer.produce('Test 1', topic: topic1, partition: 0)
    producer.produce('Test 2', topic: topic1, partition: 1)
    producer.deliver_messages
    producer.produce('Test 3', topic: topic2, partition: 0)
    producer.produce('Test 4', topic: topic2, partition: 1)
    producer.produce('Test 5', topic: topic2, partition: 2)
    producer.deliver_messages
    producer.abort_transaction

    records = kafka.fetch_messages(topic: topic1, partition: 0, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic1, partition: 1, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic2, partition: 0, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic2, partition: 1, offset: :earliest)
    expect(records.length).to eql(0)

    records = kafka.fetch_messages(topic: topic2, partition: 2, offset: :earliest)
    expect(records.length).to eql(0)

    producer.shutdown
  end

  example 'Fenced-out producer' do
    transactional_id = SecureRandom.uuid
    topic = create_random_topic(num_partitions: 3)

    producer1 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )

    producer1.init_transactions
    producer1.begin_transaction
    producer1.produce('Test 1', topic: topic, partition: 0)
    producer1.produce('Test 2', topic: topic, partition: 1)
    producer1.produce('Test 3', topic: topic, partition: 2)
    producer1.deliver_messages
    producer1.commit_transaction

    sleep 1

    producer2 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )
    producer2.init_transactions
    producer2.begin_transaction
    producer2.produce('Test 4', topic: topic, partition: 0)
    producer2.produce('Test 5', topic: topic, partition: 1)
    producer2.deliver_messages
    producer2.commit_transaction

    producer1.begin_transaction
    producer1.produce('Test 6', topic: topic, partition: 0)
    expect do
      producer1.deliver_messages
    end.to raise_error(Kafka::InvalidProducerEpochError)

    producer1.shutdown
    producer2.shutdown
  end

  example 'Concurrent transaction' do
    transactional_id = SecureRandom.uuid
    topic = create_random_topic(num_partitions: 3)

    producer1 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )

    producer1.init_transactions
    producer1.begin_transaction
    producer1.produce('Test 1', topic: topic, partition: 0)
    producer1.produce('Test 2', topic: topic, partition: 1)
    producer1.produce('Test 3', topic: topic, partition: 2)
    producer1.deliver_messages

    producer2 = kafka.producer(
      transactional: true,
      transactional_id: transactional_id
    )
    expect do
      producer2.init_transactions
    end.to raise_error(Kafka::ConcurrentTransactionError)

    begin
      producer1.shutdown
      producer2.shutdown
    rescue;
    end
  end

  example '#transaction syntax' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    producer.transaction do
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

  example '#transaction syntax, block raises error' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    expect do
      producer.transaction do
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

  example '#transaction syntax, block actively aborts the transaction' do
    producer = kafka.producer(
      transactional: true,
      transactional_id: SecureRandom.uuid
    )

    topic = create_random_topic(num_partitions: 3)

    producer.init_transactions
    expect do
      producer.transaction do
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
end
