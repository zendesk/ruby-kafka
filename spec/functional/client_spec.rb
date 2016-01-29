describe "Producer API", type: :functional do
  let(:logger) { Logger.new(log) }
  let(:log) { LOG }
  let(:kafka) { Kafka.new(seed_brokers: KAFKA_BROKERS, client_id: "test", logger: logger) }

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
