# frozen_string_literal: true

def with_retry(opts = {}, &block)
  retries = opts[:retry] || 3
  retry_wait = opts[:retry_wait] || 1
  tries = 0
  begin
    yield
  rescue RSpec::Expectations::ExpectationNotMetError => e
    sleep retry_wait
    tries += 1
    retry if tries <= retries
    raise e
  end
end

describe "Topic management API", functional: true do
  example "creating topics" do
    topic = generate_topic_name
    expect(kafka.topics).not_to include(topic)

    kafka.create_topic(topic, num_partitions: 3)

    partitions = kafka.partitions_for(topic)

    expect(partitions).to eq 3
  end

  example "creating a topic with config entries" do
    unless kafka.supports_api?(Kafka::Protocol::DESCRIBE_CONFIGS_API)
      skip("This Kafka version not support ")
    end

    topic = generate_topic_name
    expect(kafka.topics).not_to include(topic)

    kafka.create_topic(topic, num_partitions: 3, config: { "cleanup.policy" => "compact" })

    configs = kafka.describe_topic(topic, ["cleanup.policy"])
    expect(configs.fetch("cleanup.policy")).to eq("compact")
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
    with_retry {
      kafka.refresh_metadata
      expect(kafka.partitions_for(topic)).to eq 10
    }
  end

  example "describe a topic" do
    unless kafka.supports_api?(Kafka::Protocol::DESCRIBE_CONFIGS_API)
      skip("This Kafka version not support DescribeConfig API")
    end

    topic = generate_topic_name
    kafka.create_topic(topic, num_partitions: 3)
    configs = kafka.describe_topic(topic, %w(retention.ms))

    expect(configs.keys).to eql(%w(retention.ms))
    expect(configs['retention.ms']).to be_a(String)
  end

  example "alter a topic configuration" do
    unless kafka.supports_api?(Kafka::Protocol::ALTER_CONFIGS_API)
      skip("This Kafka version not support AlterConfig API")
    end

    topic = generate_topic_name
    kafka.create_topic(topic, num_partitions: 3)
    kafka.alter_topic(
      topic,
      'retention.ms' => 1234567,
      'max.message.bytes' => '987654'
    )

    retention_configs = kafka.describe_topic(topic, %w(retention.ms))
    expect(retention_configs['retention.ms']).to eql('1234567')
    max_msg_bytes_configs = kafka.describe_topic(topic, %w(max.message.bytes))
    expect(max_msg_bytes_configs['max.message.bytes']).to eql('987654')
  end
end
