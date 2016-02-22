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

  it "only compresses the messages if there are at least the configured threshold" do
    message1 = Kafka::Protocol::Message.new(value: "hello1" * 100)
    message2 = Kafka::Protocol::Message.new(value: "hello2" * 100)

    compressed_message_set = Kafka::Protocol::MessageSet.new(
      messages: [message1, message2],
      compression_codec: Kafka::SnappyCodec.new,
      compression_threshold: 2, # <-- only requires two messages for compression.
    )

    uncompressed_message_set = Kafka::Protocol::MessageSet.new(
      messages: [message1, message2],
      compression_codec: Kafka::SnappyCodec.new,
      compression_threshold: 3, # <-- requires three messages for compression.
    )

    compressed_data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)
    uncompressed_data = Kafka::Protocol::Encoder.encode_with(uncompressed_message_set)

    expect(compressed_data.bytesize).to be < uncompressed_data.bytesize
  end
end
