# frozen_string_literal: true

require "kafka/datadog"
require "socket"
require "fake_datadog_agent"

describe "Reporting metrics to Datadog", functional: true do
  let(:agent) { FakeDatadogAgent.new }

  before do
    agent.start
  end

  after do
    agent.stop
  end

  example "reporting connection metrics using UDP socket" do
    Kafka::Datadog.port = agent.port

    kafka.topics

    agent.wait_for_metrics(count: 4)

    expect(agent.metrics).to include("ruby_kafka.api.calls")
    expect(agent.metrics).to include("ruby_kafka.api.latency")
    expect(agent.metrics).to include("ruby_kafka.api.request_size")
    expect(agent.metrics).to include("ruby_kafka.api.response_size")
  end

  example "reporting connection metrics using Unix domain socket" do
    Kafka::Datadog.socket_path = agent.socket_path

    kafka.topics

    agent.wait_for_metrics(count: 4)

    expect(agent.metrics).to include("ruby_kafka.api.calls")
    expect(agent.metrics).to include("ruby_kafka.api.latency")
    expect(agent.metrics).to include("ruby_kafka.api.request_size")
    expect(agent.metrics).to include("ruby_kafka.api.response_size")
  end
end
