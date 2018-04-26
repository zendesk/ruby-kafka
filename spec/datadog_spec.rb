# frozen_string_literal: true

require "kafka/datadog"
require "fake_datadog_agent"

describe Kafka::Datadog do
  let(:agent) { FakeDatadogAgent.new }

  before do
    agent.start
  end

  after do
    agent.stop
  end

  it "emits metrics to the Datadog agent" do
    Kafka::Datadog.host = agent.host
    Kafka::Datadog.port = agent.port

    client = Kafka::Datadog.statsd

    client.increment("greetings")

    agent.wait_for_metrics

    expect(agent.metrics.count).to eq 1

    metric = agent.metrics.first

    expect(metric).to eq "ruby_kafka.greetings"
  end
end
