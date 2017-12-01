describe Kafka::Protocol::ConsumerGroupProtocol do
  it "encodes and decodes" do
    group = Kafka::Protocol::ConsumerGroupProtocol.new(
      version: 1,
      topics: ["test"],
      user_data: "foobar"
    )

    io = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(io)
    group.encode(encoder)
    data = StringIO.new(io.string)
    decoder = Kafka::Protocol::Decoder.new(data)

    group2 = Kafka::Protocol::ConsumerGroupProtocol.decode(decoder)
    expect(group.version).to eq(group2.version)
    expect(group.topics).to eq(group2.topics)
    expect(group.user_data).to eq(group2.user_data)
  end
end
