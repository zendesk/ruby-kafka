describe Kafka::Protocol::Message do
  include Kafka::Protocol

  it "decodes message sets" do
    message1 = Kafka::Protocol::Message.new(value: "hello")
    message2 = Kafka::Protocol::Message.new(value: "good-day")

    message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind

    decoder = Kafka::Protocol::Decoder.new(data)
    new_message_set = Kafka::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1, message2]
  end

  it "skips the last message if it has been truncated" do
    message1 = Kafka::Protocol::Message.new(value: "hello")
    message2 = Kafka::Protocol::Message.new(value: "good-day")

    message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])

    data = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = Kafka::Protocol::Decoder.new(data)
    new_message_set = Kafka::Protocol::MessageSet.decode(decoder)

    expect(new_message_set.messages).to eq [message1]
  end

  it "raises MessageTooLargeToRead if the first message in the set has been truncated" do
    message = Kafka::Protocol::Message.new(value: "hello")

    message_set = Kafka::Protocol::MessageSet.new(messages: [message])

    data = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(data)

    message_set.encode(encoder)

    data.rewind
    data.truncate(data.size - 1)

    decoder = Kafka::Protocol::Decoder.new(data)

    expect {
      Kafka::Protocol::MessageSet.decode(decoder)
    }.to raise_exception(Kafka::MessageTooLargeToRead)
  end
end
