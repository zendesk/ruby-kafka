require "timecop"

describe "Producer API", functional: true do
  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "listing all topics in the cluster" do
    expect(kafka.has_topic?(topic)).to eq true

    topic2 = create_random_topic

    expect(kafka.has_topic?(topic2)).to eq true
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
        kafka.deliver_message("yolo", topic: topic, key: "xoxo", partition: 0)
      end
    }.to raise_exception(Kafka::DeliveryFailed) {|exception|
      expect(exception.failed_messages).to eq [Kafka::PendingMessage.new("yolo", "xoxo", topic, 0, nil, now)]
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

    kafka.deliver_message("hello", topic: topic, partition: 0, retries: 2)
    kafka.deliver_message("world", topic: topic, partition: 0, retries: 2)

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
end
