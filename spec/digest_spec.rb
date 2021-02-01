# frozen_string_literal: true

describe Kafka::Digest do
  describe "crc32" do
    let(:digest) { Kafka::Digest.find_digest(:crc32) }

    it "is supported" do
      expect(digest).to be_truthy
    end

    it "produces hash for value" do
      expect(digest.hash("yolo")).to eq(1623057525)
    end
  end

  describe "murmur2" do
    let(:digest) { Kafka::Digest.find_digest(:murmur2) }

    it "is supported" do
      expect(digest).to be_truthy
    end

    it "produces hash for value" do
      expect(digest.hash("yolo")).to eq(1633766415)
    end
  end

  describe "unknown hash function" do
    it "raises" do
      expect { Kafka::Digest.find_digest(:yolo) }.to raise_error
    end
  end
end
