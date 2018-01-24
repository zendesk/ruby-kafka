describe "Topic management API", functional: true do
  example "creating topics" do
    topic = generate_topic_name
    expect(kafka.topics).not_to include(topic)

    kafka.create_topic(topic, num_partitions: 3)

    partitions = kafka.partitions_for(topic)

    expect(partitions).to eq 3
  end

  example "deleting topics" do
    topic = generate_topic_name

    kafka.create_topic(topic, num_partitions: 3)
    expect(kafka.partitions_for(topic)).to eq 3

    kafka.delete_topic(topic)
    expect(kafka.has_topic?(topic)).to eql(false)
  end

  example "create partitions" do
    unless kafka.supports_api?(Kafka::Protocol::CREATE_PARTITIONS_API)
      skip("This Kafka version not support ")
    end
    topic = generate_topic_name

    kafka.create_topic(topic, num_partitions: 3)
    expect(kafka.partitions_for(topic)).to eq 3

    kafka.create_partitions_for(topic, num_partitions: 10)
    expect(kafka.partitions_for(topic)).to eq 10
  end

  example "validate partition creation" do
    unless kafka.supports_api?(Kafka::Protocol::CREATE_PARTITIONS_API)
      skip("This Kafka version not support ")
    end
    topic = generate_topic_name

    kafka.create_topic(topic, num_partitions: 3)
    expect(kafka.partitions_for(topic)).to eq 3

    kafka.create_partitions_for(topic, num_partitions: 10, validate_only: true)
    expect(kafka.partitions_for(topic)).to eq 3
  end
end
