describe "Producer API", functional: true do
  let(:kafka) do
    Kafka.new(
      seed_brokers: KAFKA_BROKERS,
      logger: Logger.new(LOG),
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
end
