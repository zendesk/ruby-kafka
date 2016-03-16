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

  example "listing all topics in the cluster" do
    expect(kafka.topics).to include "test-messages"
  end

  example "fetching the partition count for a topic" do
    expect(kafka.partitions_for("test-messages")).to eq 3
  end
end
