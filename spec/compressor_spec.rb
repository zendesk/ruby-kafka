# frozen_string_literal: true

describe Kafka::Compressor do
  describe ".compress" do
    let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }

    context 'legacy message set' do
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

    context 'record batch' do
      it "only compresses the messages if there are at least the configured threshold" do
        compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 3, instrumenter: instrumenter)

        record1 = Kafka::Protocol::Record.new(value: "hello1")
        record2 = Kafka::Protocol::Record.new(value: "hello2")

        record_batch = Kafka::Protocol::RecordBatch.new(records: [record1, record2])
        compressed_record_batch = compressor.compress(record_batch)
        expect(record_batch.codec_id).to eql(0)

        expect(compressed_record_batch).to eq Kafka::Protocol::Encoder.encode_with(record_batch)
      end

      it "reduces the data size" do
        compressor = Kafka::Compressor.new(codec_name: :snappy, threshold: 1, instrumenter: instrumenter)
        codec = Kafka::SnappyCodec.new

        record_batch = Kafka::Protocol::RecordBatch.new(
          records: (0..10).map do |index|
            Kafka::Protocol::Record.new(value: "hello#{index}")
          end
        )
        compressed_record_batch = compressor.compress(record_batch)

        record_batch.codec_id = codec.codec_id
        uncompressed_data = Kafka::Protocol::Encoder.encode_with(record_batch)

        expect(compressed_record_batch.bytesize).to be < uncompressed_data.bytesize
      end
    end
  end
end
