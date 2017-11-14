require "kafka/datadog"
require "fake_datadog_agent"

describe Kafka::Datadog do
  let(:agent) { FakeDatadogAgent.new }

  before do
    agent.start

    Kafka::Datadog.host = agent.host
    Kafka::Datadog.port = agent.port
  end

  after do
    agent.stop
  end

  it "emits metrics to the Datadog agent" do
    client = Kafka::Datadog.statsd
    client.increment("greetings")

    agent.wait_for_metrics

    expect(agent.metrics.count).to eq 1

    metric = agent.metrics.first

    expect(metric).to eq "ruby_kafka.greetings"
  end

  it "subscribes to ruby-kafka notifications" do
    payload = {
      client_id: "yolo",
      group_id: "yolo-group",
    }

    ActiveSupport::Notifications.instrument("sync_group.consumer.kafka", payload)

    agent.wait_for_metrics

    expect(agent.metrics.count).to eq 1

    metric = agent.metrics.first

    expect(metric).to eq "ruby_kafka.consumer.sync_group"
  end
end
