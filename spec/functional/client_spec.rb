# frozen_string_literal: true

require "timecop"

describe "Client API", functional: true do
  let!(:topic) { create_random_topic(num_partitions: 3) }
  let!(:deleted_topic) { create_random_topic(num_partitions: 3) }

  before do
    kafka.delete_topic(deleted_topic)
  end

  example "listing available topics in the cluster" do
    # Use a clean Kafka instance to avoid hitting caches.
    kafka = Kafka.new(KAFKA_BROKERS, logger: LOGGER)

    topics = kafka.topics

    expect(topics).to include topic
    expect(topics).not_to include deleted_topic
    expect(kafka.has_topic?(topic)).to eq true
    expect(kafka.has_topic?(deleted_topic)).to eq false
  end

  example "listing brokers in the cluster" do
    brokers = kafka.brokers
    controller = kafka.controller_broker

    expect(brokers).to include controller
  end

  example "listing consumer groups working in the cluster" do
    kafka = Kafka.new(KAFKA_BROKERS, logger: LOGGER)

    group_id = "consumer-group-#{rand(1000)}"
    kafka.deliver_message('test', topic: topic)
    consumer = kafka.consumer(group_id: group_id)
    consumer.subscribe(topic)
    consumer.each_message do |msg|
      consumer.stop
    end

    expect(kafka.groups).to include(group_id)
  end

  example "describing consumer group with active consumer" do
    group_id = "consumer-group=#{rand(1000)}"

    kafka.deliver_message('test', topic: topic)
    consumer = kafka.consumer(group_id: group_id)
    consumer.subscribe(topic)

    result = nil
    consumer.each_message do |msg|
      result = kafka.describe_group(group_id)
      consumer.stop
    end

    expect(result.state).to eq('Stable')
    expect(result.protocol).to eq('standard')
    expect(result.members.count).to eq(1)

    member = result.members.first
    expect(member.client_id).to_not be_nil
    expect(member.client_host).to_not be_nil
    expect(member.member_id).to_not be_nil
    expect(member.member_assignment).to be_an_instance_of(Kafka::Protocol::MemberAssignment)
    expect(member.member_assignment.topics[topic].sort).to eq([0, 1, 2])
  end

  example "describing non-existent consumer group" do
    group_id = "consumer-group=#{rand(1000)}"
    result = kafka.describe_group(group_id)
    expect(result.state).to eq('Dead')
    expect(result.protocol).to be_empty
    expect(result.members).to be_empty
  end

  example "describing an inactive consumer group" do
    group_id = "consumer-group=#{rand(1000)}"

    kafka.deliver_message('test', topic: topic)
    consumer = kafka.consumer(group_id: group_id)
    consumer.subscribe(topic)
    consumer.each_message do |msg|
      consumer.stop
    end

    result = kafka.describe_group(group_id)
    expect(result.state).to eq('Empty')
    expect(result.protocol).to be_empty
    expect(result.members).to be_empty
  end

  example "fetching the partition count for a topic" do
    expect(kafka.partitions_for(topic)).to eq 3
  end

  example "fetching the partition count for a topic that doesn't yet exist" do
    topic = "unknown-topic-#{rand(1000)}"

    expect { kafka.partitions_for(topic) }.to raise_exception(Kafka::LeaderNotAvailable)

    # Eventually the call should succeed.
    expect {
      10.times { kafka.partitions_for(topic) rescue nil }
    }.not_to raise_exception

    expect(kafka.partitions_for(topic)).to be > 0
  end

  example "delivering a message to a topic" do
    kafka.deliver_message("yolo", topic: topic, key: "xoxo", partition: 0)

    message = kafka.fetch_messages(topic: topic, partition: 0, offset: 0).first

    expect(message.value).to eq "yolo"
    expect(message.key).to eq "xoxo"
  end

  example "delivering a message to a topic that doesn't yet exist" do
    topic = "unknown-topic-#{rand(1000)}"
    now = Time.now

    expect {
      Timecop.freeze(now) do
        kafka.deliver_message("yolo", topic: topic, key: "xoxo", partition: 0, headers: { hello: 'World' })
      end
    }.to raise_exception(Kafka::DeliveryFailed) {|exception|
      expect(exception.failed_messages).to eq [
        Kafka::PendingMessage.new(
          value: "yolo",
          key: "xoxo",
          headers: {
            hello: "World",
          },
          topic: topic,
          partition: 0,
          partition_key: nil,
          create_time: now
        )
      ]
    }
  end

  example "enumerating the messages in a topic" do
    values = (1..10).to_a

    values.each do |value|
      kafka.deliver_message(value.to_s, topic: topic)
    end

    kafka.each_message(topic: topic) do |message|
      value = Integer(message.value)
      values.delete(value)

      if message.value == "5"
        values << 666
        kafka.deliver_message("666", topic: topic)
      end

      break if values.empty?
    end

    expect(values).to eq []
  end

  example "getting the last offset for a topic partition" do
    topic = create_random_topic(num_partitions: 1, num_replicas: 1)

    kafka.deliver_message("hello", topic: topic, partition: 0)
    kafka.deliver_message("world", topic: topic, partition: 0)

    offset = kafka.last_offset_for(topic, 0)

    expect(offset).to eq 1
  end

  example "getting the last offsets for a topic" do
    topic = create_random_topic(num_partitions: 2, num_replicas: 1)

    kafka.deliver_message("hello", topic: topic, partition: 0)
    kafka.deliver_message("world", topic: topic, partition: 0)
    kafka.deliver_message("hello", topic: topic, partition: 1)
    kafka.deliver_message("world", topic: topic, partition: 1)

    offsets = kafka.last_offsets_for(topic)

    expect(offsets[topic][0]).to eq 1
    expect(offsets[topic][1]).to eq 1
  end

  example 'support record headers' do
    topic = create_random_topic(num_partitions: 1, num_replicas: 1)

    kafka.deliver_message(
      "hello", topic: topic,
      headers: { hello: 'World', 'greeting' => 'is great', bye: 1, love: nil }
    )

    sleep 0.2
    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages[0].value).to eq "hello"
    expect(messages[0].headers).to eql(
      'hello' => 'World',
      'greeting' => 'is great',
      'bye' => '1',
      'love' => ''
    )
  end
end
