# frozen_string_literal: true

require "kafka/datadog"
require "socket"
require "fake_datadog_agent"

describe "Reporting metrics to Datadog", functional: true do
  example "reporting connection metrics" do
    agent = FakeDatadogAgent.new

    Kafka::Datadog.port = agent.port

    agent.start

    kafka.topics

    agent.wait_for_metrics(count: 4)

    expect(agent.metrics).to include("ruby_kafka.api.calls")
    expect(agent.metrics).to include("ruby_kafka.api.latency")
    expect(agent.metrics).to include("ruby_kafka.api.request_size")
    expect(agent.metrics).to include("ruby_kafka.api.response_size")
  end
end
