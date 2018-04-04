describe Kafka::BrokerUri do
  it "accepts valid seed brokers URIs" do
    expect(Kafka::BrokerUri.parse("kafka://hello").to_s).to eq "kafka://hello:9092"
    expect(Kafka::BrokerUri.parse("kafka+ssl://hello").to_s).to eq "kafka+ssl://hello:9092"
  end

  it "maps plaintext:// to kafka://" do
    expect(Kafka::BrokerUri.parse("PLAINTEXT://kafka").scheme).to eq "kafka"
  end

  it "maps ssl:// to kafka+ssl://" do
    expect(Kafka::BrokerUri.parse("SSL://kafka").scheme).to eq "kafka+ssl"
  end

  it "raises Kafka::Error on invalid schemes" do
    expect {
      Kafka::BrokerUri.parse("http://kafka")
    }.to raise_exception(Kafka::Error, "invalid protocol `http` in `http://kafka`")
  end
end
