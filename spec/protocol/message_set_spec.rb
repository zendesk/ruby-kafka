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

    expect(new_message_set).to eq message_set
  end
end
