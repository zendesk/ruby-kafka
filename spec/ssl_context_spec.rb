# frozen_string_literal: true

describe Kafka::SslContext do
  it "raises ArgumentError if a client cert but no client cert key is passed" do
    expect {
      Kafka::SslContext.build(client_cert: "hello", client_cert_key: nil)
    }.to raise_exception(ArgumentError)
  end

  it "raises ArgumentError if a client cert key but no client cert is passed" do
    expect {
      Kafka::SslContext.build(client_cert: nil, client_cert_key: "hello")
    }.to raise_exception(ArgumentError)
  end
end
