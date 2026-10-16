# frozen_string_literal: true

describe Kafka::Protocol::DeleteTopicsResponse do
  it "decodes v1 responses with throttle time" do
    bytes = "\x00\x00\x00\x64" + # throttle_time_ms
      "\x00\x00\x00\x02" + # two topics
      "\x00\x03one\x00\x00" +
      "\x00\x03two\x00\x03"
    buffer = StringIO.new(bytes)

    response = described_class.decode(Kafka::Protocol::Decoder.new(buffer))

    expect(response.errors).to eq([["one", 0], ["two", 3]])
    expect(buffer).to be_eof
  end
end
