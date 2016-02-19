describe Kafka::Protocol::MessageSet do
  it "encodes and decodes compressed messages" do
    message1 = Kafka::Protocol::Message.new(value: "hello1")
    message2 = Kafka::Protocol::Message.new(value: "hello2")

    message_set = Kafka::Protocol::MessageSet.new(
      messages: [message1, message2],
      compression_codec: Kafka::SnappyCodec.new,
    )

    encoded_data = Kafka::Protocol::Encoder.encode_with(message_set)
    decoder = Kafka::Protocol::Decoder.from_string(encoded_data)

    decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)

    expect(decoded_message_set.messages.map(&:value)).to eq ["hello1", "hello2"]
  end
end
