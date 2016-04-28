describe "Producer API", functional: true do
  let(:kafka) do
    Kafka.new(
      seed_brokers: KAFKA_BROKERS,
      logger: LOGGER,
      connect_timeout: 0.1,
      socket_timeout: 0.1,
    )
  end

  before do
    require "test_cluster"
  end

  after do
    kafka.close
  end

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
end
