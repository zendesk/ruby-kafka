# frozen_string_literal: true

describe Kafka::Protocol::DescribeConfigsResponse do
  it "decodes v1 config sources and consumes synonyms before the next config" do
    bytes = "\x00\x00\x00\x64\x00\x00\x00\x01" + # throttle time, one resource
      "\x00\x00\xFF\xFF\x02\x00\x03one" + # error, message, topic resource
      "\x00\x00\x00\x02" + # two configs
      "\x00\x01a\x00\x01b\x00\x01\x00" + # dynamic topic config
      "\x00\x00\x00\x01\x00\x01a\xFF\xFF\x05" + # one synonym with null value
      "\x00\x01c\xFF\xFF\x01\x05\x01\x00\x00\x00\x00" # default config, no synonyms
    buffer = StringIO.new(bytes)

    response = described_class.decode(Kafka::Protocol::Decoder.new(buffer))

    resource = response.resources.first
    expect(resource.name).to eq("one")
    expect(resource.type).to eq(:topic)
    expect(resource.error_code).to eq(0)
    expect(resource.error_message).to be_nil
    first, second = resource.configs
    expect([first.name, first.value, first.read_only, first.is_default, first.is_sensitive]).to eq(["a", "b", false, false, false])
    expect([second.name, second.value, second.read_only, second.is_default, second.is_sensitive]).to eq(["c", nil, true, true, true])
    expect(buffer).to be_eof
  end
end
