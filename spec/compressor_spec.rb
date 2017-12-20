describe Kafka::Compressor do
  describe ".compress" do
    let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }

    it "encodes and decodes compressed messages" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1")
      message2 = Kafka::Protocol::Message.new(value: "hello2")

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)
      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message = Kafka::Protocol::Message.decode(decoder)
      decoded_message_set = decoded_message.decompress
      messages = decoded_message_set.messages

      expect(messages.map(&:value)).to eq ["hello1", "hello2"]

      # When decoding a compressed message, the offsets are calculated relative to that
      # of the container message. The broker will set the offset in normal operation,
      # but at the client-side we set it to -1.
      expect(messages.map(&:offset)).to eq [-2, -1]
    end

    it "sets offsets correctly for compressed messages with relative offsets" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(value: "hello1", offset: 0)
      message2 = Kafka::Protocol::Message.new(value: "hello2", offset: 1)
      message3 = Kafka::Protocol::Message.new(value: "hello3", offset: 772043)

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2, message3])
      compressed_message_set = compressor.compress(message_set)
      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)
      messages = decoded_message_set.messages

      expect(messages.map(&:offset)).to eq [772041, 772042, 772043]
    end

    it "keeps the offsets for compressed messages with provided offsets" do
      compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)

      message1 = Kafka::Protocol::Message.new(
        value: "\x00\x00\x00\x03\\\bkred(2017-12-19T00:22:04Z\x00H1a89d4c5-b032-4fe5-a975-e066fea110fdH72b7dffe-3a08-485f-8406-978de840b66e(2017-12-19T00:22:03Z\"11203403035863172\x00\x06\x00\"11203403035863172\x00\xAC\x01\x02\x142017-12-18\x00\x02\x00\x02",
        codec_id: 0,
        key: "11203403035863172",
        offset: 772033,
      )
      message2 = Kafka::Protocol::Message.new(
        value: "\x1F\x8B\b\x00\x00\x00\x00\x00\x00\x00\x8D\x96;\xAB\\U\x14\xC7On4\x19L,\x02\x96b\x11,\x14\\\xB0\xD7c\xEF\xBDv\xF0\x03\x8C\x95\bV\xC1f?\e\xC1\xC2\x0F\xA0\xAD_@%\xB6Z\xE5\x13\x98p\xB1\xF0\x91\x87ZJ|\xE1\xB5\x89\x8D\xD8\x88\xD8\xA8\x04\xD7\xB9\xC9Mr's\x98\xCCc\xC3\xFC\x999\xAC\xDFZ\xFF\xF5?3M\xF68s\xFDs;\x7F\xB8r\xAD\xAE\xE6\x8F\xD39DtAP\xBD:U\xD1\x18L\xBCb\xEF\x93o\xAC\xDE|\xBB\xB7\x17\xC8a\x04$\xC0\xF4\xBAs\x17\x88.8\xB98\xAD}p\xA1\xF9<\x80\xA2D\x90\xDE\x152\xFA\x01u\xD4L.f\xA2\x8E\xEB\xA8XH\xA2\x03\x9D\xAF!\xDDEH\x8E\x03\x8C\xE2\x87\xF7-\x17\xEA\xB4\xE5\xFA|\xF1\xFC\xA3E\x9D\x9D\xB6\x88W\xFD\xDE3\xF7\x7F\xAF\xD3\x9E=\xEF2~a\xE7\xF7\x7F\x1D|p\xE6\x88\x91\x91$EUf\x8C\x1C\x9D\x89\x9F\xEEdTm#\xD6\xE0A[/ \x85\x10r\x1EV~nY[uu4Y\x17\x1DA\xC91`\x1D\b\x82\xC5C.\xCC\xD0\x06c\xCF\x14\xD9\x9A\xB4\xC4\xB8Y\xD4\xA9i\x8B8- ~i\xE7\x8F\xEF\\z\xF9\xDD#D\xEFD\x1C\xCF\xEDI\xE2c4\xF1\xEAN\xC4\x96\xB8\x06t\x04$\xA5\x81$mP\xD4&\x14Gs\xD9&\x87%\xD5\xB5\xC3\xAA\x9D\xA5\x82o5\x81T\x1DP\x12{\xE8)G\x1C\x9D=\x93.!n\x16u\x88\xB8)^\xFB\xED\xE4\x02\xE4W3\xE4'\xEB[\x1F=\xF0\xAA\x17VN\x91\x03\xC5C\xAF\xEE\x86\xB4V\x86^e\x1EL\xA9 \xE2\x19r\x1Ad$9Sk\xC8\xBE\xB4\xB5+\xA5q\xF3\n\xA56\x83\xCC\xB9B\x1E\xD6\x133\xF5\xE0\x16sT\xF2\xCB^=^\xD4\xA9i\x8B\xF8\xDD\x9D\xD3\v\x90\xD7\xEC\xFC\xE5\xC3\xE7\x0E\xFE\xBCoV\xFB\xA9 \xA9\xA2\xB7\xFE\xB0\x89\x9F\xED\x84\fT*\xA2Y\xD0c\x9E\x87d\x8E,iD\xE8~`\xEF.\xF7\xDA\xE2:'\xCCC\x82@\x91. #\xCD\x8Ev\xC1\xDA\xD1\x8Bm\xAEr)i\xD1\xAC\eE=5m\x11o\x7F\xFB\xFC\xEA!\xC8\xFF\xEE\x9CX\xCD\xEF{\xA0\xD7\xED\xFC\xF9\xD7[/\xBEr\x04J\xE8\xC4\xDC\xCE\x14\x84}\xF4&\xEE\xEF\x06u\xA95\x9F\x82\xED\x9A\xAD\x99\xE4\x92!\x8D\x90a\xF4H\x91\x90\xB3\xE7\xB2n\xF3\xECbc\b\xCA\x06\xDAJ\x82\x1Cc\x81\x14R\x1A\xA86\x9B\xE6\x96@7\x8Bzb\xDA\"\xBE\xF7\xFB\xB1i\xDExu\xCF^\xF70o\xCC\x01\xFB\xEF%\xBC}\x84\x19\xACK\xA8v\x013\xA2\x1Eb\xEE\x0EX\xAE\x12S\v\x11\xB8\x8Bm&c6gJ\x06O19\"\xE7,\x1C\xCC\xB4\xD2(T\x01\xD5`~F;TR6\xD36\xB1\x14\x0Eu\x84\xC5\xF0\xD9,j5m\x11/\x9F_\xF0\xECM;o\x9E\xFD\xFB\xAD\xD3\x0F<\x8B\xE2-\xEC\x95\xA3\xD7H&^\xDE\xC1\xB8Z;\xDB\x0F\xFB.C'\x9Dw\x8E\xBC1R\x81\xC2\x8D, \x18K\xC3\xB5$\xC1\x18\xB0\x81\v\xDEF\x99,\x85\x95\x8AZ\xC0\xBA\x1E\vS\xAB\x1D\x97={\xBC\xA8S\xAB-\xE2]\xA4\x19\xEA\xEB\xD9\x9F/\xBD\xFF\xDA\xFE2\xD4n\x7F\x8E\x91B\xB5>\x82\x05\x9C\xF9S,-\x93\xF5\x12\xA82\x8DQS\x0F\xC4\xEB\xD9\x8C\xB5\x94\x0E\xDC\xCC\xBA\"d;\xE8b\x03\xEFl\xA3R\x14*\xB9<.\xD4j\xDA\"\xFE\xF3\xD3\x93\x0FO\xEE\x8Fs{\xF6\xBA\x87\xF9\xCD\x1C\xAA'\x0E\x9E}\xFA\bS\\2k+\xCF\x99\x8C\x8Fy\xE7\xE8V&\x92\xED\xD5\x98Cg\xBE\e[\xDE8\a!ga\xD7\xA9\x88\xA4u\x95\xE1\xEC\xF6\xD0\xA1g\xD7A\x9C\x8D-\xB7T \x06\xEE9kV\xFB\xA7\xB0\x84\xB9Y\xD4a\xA8n\x8A\xFB\x1F?r\xE7\xF8\x1F\xB0l\x06\x04\xE6\b\x00\x00",
        codec_id: 1,
        key: nil,
        offset: 772043,
      )

      message_set = Kafka::Protocol::MessageSet.new(messages: [message1, message2])
      compressed_message_set = compressor.compress(message_set)

      data = Kafka::Protocol::Encoder.encode_with(compressed_message_set)

      decoder = Kafka::Protocol::Decoder.from_string(data)
      decoded_message_set = Kafka::Protocol::MessageSet.decode(decoder)
      messages = decoded_message_set.messages

      expect(messages.map(&:offset)).to eq [772033, 772034, 772035, 772036, 772037, 772038, 772039, 772040, 772041, 772042, 772043]
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
