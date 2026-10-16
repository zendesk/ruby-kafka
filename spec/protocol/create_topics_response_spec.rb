# frozen_string_literal: true

describe Kafka::Protocol::CreateTopicsResponse do
  it "decodes v2 responses with throttle time and per-topic error messages" do
    bytes = "\x00\x00\x00\x64" + # throttle_time_ms
      "\x00\x00\x00\x02" + # two topics
      "\x00\x03one\x00\x00\xFF\xFF" +
      "\x00\x03two\x00\x24\x00\x06exists"
    buffer = StringIO.new(bytes)

    response = described_class.decode(Kafka::Protocol::Decoder.new(buffer))

    expect(response.errors).to eq([["one", 0], ["two", 36]])
    expect(buffer).to be_eof
  end
end
