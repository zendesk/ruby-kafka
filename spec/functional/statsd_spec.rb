# frozen_string_literal: true

require "kafka/statsd"
require "socket"
require "fake_statsd_agent"

describe "Reporting metrics to Statsd", functional: true do
  example "reporting connection metrics" do
    agent = FakeStatsdAgent.new

    Kafka::Statsd.port = agent.port

    agent.start

    kafka.topics

    agent.wait_for_metrics(count: 4)

    expect(agent.metrics).to include(/ruby_kafka\.api\.\w+\.\w+\.[\w\.]+\.calls/)
    expect(agent.metrics).to include(/ruby_kafka\.api\.\w+\.\w+\.[\w\.]+\.latency/)
    expect(agent.metrics).to include(/ruby_kafka\.api\.\w+\.\w+\.[\w\.]+\.request_size/)
    expect(agent.metrics).to include(/ruby_kafka\.api\.\w+\.\w+\.[\w\.]+\.response_size/)
  end
end
