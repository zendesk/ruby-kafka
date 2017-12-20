describe Kafka::Compressor do
  describe ".compress" do
    let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }

    it "sets offsets correctly for compressed messages with relative offsets" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 1)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 2)

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2, message3])
      compressed_message_set = compressor.compress(message_set, offset: 1000)
      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)
      messages = decoded_message_set.messages

      expect(messages.map(&:offset)).to eq [998, 999, 1000]
    end

    it "sets offsets correctly for compressed messages with relative offsets on a compacted topic" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 2)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 3)

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2, message3])
      compressed_message_set = compressor.compress(message_set, offset: 1000)
      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)
      messages = decoded_message_set.messages

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end

    it "keeps the predefined offsets for messages delivered in 0.9 format" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 997)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 999)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 1000)

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2, message3])
      compressed_message_set = compressor.compress(message_set, offset: 1000)
      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)
      messages = decoded_message_set.messages

      expect(messages.map(&:offset)).to eq [997, 999, 1000]
    end

    it "only compresses the messages if there are at least the configured threshold" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 3, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1")
      message2 = Kafka::Protocol::Message.new(value: "hello2")

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      expect(compressed_message_set.messages).to eq [message1, message2]
    end

    it "reduces the data size" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1" * 100)
      message2 = Kafka::Protocol::Message.new(value: "hello2" * 100)

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      uncompressed_data = Kafka::Protocol::Encoder.encode_with(message_set)
      compressed_data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      expect(compressed_data.bytesize).to be < uncompressed_data.bytesize
    end
  end
end
