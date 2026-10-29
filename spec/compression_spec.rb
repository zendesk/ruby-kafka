# frozen_string_literal: true

describe Kafka::Compression do
  Kafka::Compression.codecs.each do |codec_name|
    describe codec_name.to_s do
      it "encodes and decodes data" do
        data = "yolo"
        codec = Kafka::Compression.find_codec(codec_name)

        expect(codec.decompress(codec.compress(data))).to eq data
      end

      it "has a consistent codec id" do
        codec = Kafka::Compression.find_codec(codec_name)

        expect(Kafka::Compression.find_codec_by_id(codec.codec_id)).to eq codec
      end
    end
  end

  context "when a codec's gem can't be loaded" do
    let(:codec) { Kafka::Compression::CODECS_BY_NAME.fetch(:snappy) }

    before do
      allow(codec).to receive(:load).and_raise(LoadError, "snappy gem missing")
    end

    it "raises Kafka::CodecUnavailable from .find_codec_by_id, keeping the LoadError as its cause" do
      expect {
        Kafka::Compression.find_codec_by_id(codec.codec_id)
      }.to raise_error(Kafka::CodecUnavailable, "snappy gem missing") { |error|
        expect(error).to be_a(Kafka::Error)
        expect(error.cause).to be_a(LoadError)
      }
    end

    it "still raises LoadError from .find_codec, so a misconfigured producer fails at startup" do
      expect {
        Kafka::Compression.find_codec(:snappy)
      }.to raise_error(LoadError, "snappy gem missing")
    end
  end
end
