require "kafka/datadog"
require "fake_datadog_agent"

describe Kafka::Datadog do
  it "emits metrics to the Datadog agent" do
    begin
      agent = FakeDatadogAgent.new
      agent.start

      Kafka::Datadog.host = agent.host
      Kafka::Datadog.port = agent.port

      client = Kafka::Datadog.statsd

      client.increment("greetings")

      agent.wait_for_metrics

      expect(agent.metrics.count).to eq 1

      metric = agent.metrics.first

      expect(metric).to eq "ruby_kafka.greetings"
    ensure
      agent.stop
    end
  end

  it "allows setting statsd" do
    begin
      client = Kafka::Datadog.statsd

      Kafka::Datadog.statsd = "OtherClient"
      expect(Kafka::Datadog.statsd).to eq "OtherClient"
    ensure
      Kafka::Datadog.statsd = client
    end
  end
end
