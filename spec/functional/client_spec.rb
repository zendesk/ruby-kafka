describe "Producer API", functional: true do
  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "listing all topics in the cluster" do
    expect(kafka.topics).to include topic
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
end
