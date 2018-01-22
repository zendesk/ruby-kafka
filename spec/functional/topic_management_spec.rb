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

  example "describe a topic" do
    unless kafka.supports_api?(Kafka::Protocol::DESCRIBE_CONFIGS_API)
      skip("This Kafka version not support ")
    end
    configs = kafka.describe_topic(topic, %w(retention.ms retention.bytes non_exists))
    expect(configs.keys).to eql(%w(retention.ms retention.bytes))
    expect(configs['retention.ms']).to be_a(String)
    expect(configs['retention.bytes']).to be_a(String)
  end
end
