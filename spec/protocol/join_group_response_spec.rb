# frozen_string_literal: true

describe Kafka::Protocol::JoinGroupResponse do
  it "decodes v2 responses with throttle time and member metadata" do
    bytes = "\x00\x00\x00\x64" + # throttle_time_ms
      "\x00\x00\x00\x00\x00\x01" + # error_code, generation_id
      "\x00\x0Aroundrobin\x00\x01a\x00\x01a" + # protocol, leader, member
      "\x00\x00\x00\x01\x00\x01a" + # one member
      "\x00\x00\x00\x0F\x00\x00\x00\x00\x00\x01\x00\x03one\xFF\xFF\xFF\xFF"
    buffer = StringIO.new(bytes)

    response = described_class.decode(Kafka::Protocol::Decoder.new(buffer))

    expect(response.error_code).to eq(0)
    expect(response.generation_id).to eq(1)
    expect(response.group_protocol).to eq("roundrobin")
    expect(response.leader_id).to eq("a")
    expect(response.member_id).to eq("a")
    expect(response.members.fetch("a").topics).to eq(["one"])
    expect(response.members.fetch("a").user_data).to be_nil
    expect(buffer).to be_eof
  end
end
