# frozen_string_literal: true

describe Kafka::Protocol::SyncGroupResponse do
  describe ".decode" do
    subject(:response) { Kafka::Protocol::SyncGroupResponse.decode(decoder) }

    let(:decoder) { Kafka::Protocol::Decoder.new(buffer) }
    let(:buffer) { StringIO.new(response_bytes) }

    context "the response is successful" do
      let(:response_bytes) { "\x00\x00\x00\x00\x007\x00\x00\x00\x00\x00\x01\x00\x1Fsome-topic-f064d6897583eb395896\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x01\xFF\xFF\xFF\xFF" }

      it "decodes the response including the member assignment" do
        expect(response.error_code).to eq 0
        expect(response.member_assignment.topics).to eq({ "some-topic-f064d6897583eb395896" => [0, 1] })
      end
    end

    context "the response is not successful" do
      let(:response_bytes) { "\x00\x19\xFF\xFF\xFF\xFF" }

      it "decodes the response including the member assignment" do
        expect(response.error_code).to eq 25
        expect(response.member_assignment).to be_nil
      end
    end
  end
end
