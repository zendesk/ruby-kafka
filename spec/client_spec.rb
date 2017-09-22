describe Kafka::Client do
  it "accepts valid seed brokers URIs" do
    expect {
      Kafka::Client.new(seed_brokers: ["kafka://kafka"])
    }.not_to raise_exception

    expect {
      Kafka::Client.new(seed_brokers: ["kafka+ssl://kafka"])
    }.not_to raise_exception

    expect {
      Kafka::Client.new(seed_brokers: ["http://kafka"])
    }.to raise_exception(Kafka::Error, "invalid protocol `http` in `http://kafka`")
  end
end
