describe "Topic management API", functional: true do
  example "creating topics" do
    topic = generate_topic_name
    expect(kafka.topics).not_to include(topic)

    kafka.create_topic(topic, num_partitions: 3)

    partitions = kafka.partitions_for(topic)

    expect(partitions).to eq 3
  end
end
