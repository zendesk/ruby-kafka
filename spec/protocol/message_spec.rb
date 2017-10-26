describe Kafka::Protocol::Message do
  it "encodes and decodes messages" do
    message = Kafka::Protocol::Message.new(
      value: "yolo",
      key: "xx",
    )

    io = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(io)
    message.encode(encoder)
    data = StringIO.new(io.string)
    decoder = Kafka::Protocol::Decoder.new(data)

    expect(Kafka::Protocol::Message.decode(decoder)).to eq message
  end
end
