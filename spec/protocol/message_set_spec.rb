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

  describe '.decode' do
    let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }
    let(:compressor) { Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter) }

    def encode(messages: [], wrapper_message_offset: -1)
      message_set = Kafka::Protocol::MessageSet.new(messages: messages)
      compressed_message_set = compressor.compress(message_set, offset: wrapper_message_offset)
      Kafka::Protocol::Encoder.encode_with(compressed_message_set)
    end

    def decode(data)
      decoder = Kafka::Protocol::Decoder.from_string(data)
      Kafka::Protocol::MessageSet
        .decode(decoder)
        .messages
    end

    it "sets offsets correctly for compressed messages with relative offsets" do
      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 1)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 2)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [998, 999, 1000]
    end

    it "sets offsets correctly for compressed messages with relative offsets on a compacted topic" do
      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 2)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 3)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end

    it "keeps the predefined offsets for messages delivered in 0.9 format" do
      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 997)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 999)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 1000)

      data = encode(messages: [message1, message2, message3], wrapper_message_offset: 1000)
      messages = decode(data)

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end
  end
end
