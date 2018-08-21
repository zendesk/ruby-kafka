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

  it "raises ArgumentError if a client cert chain is provided but no client cert or key is passed" do
    expect {
      Kafka::SslContext.build(client_cert_chain: "HI", client_cert_key: nil, client_cert: nil)
    }.to raise_exception(ArgumentError)
  end

  it "raises ArgumentError if a client cert chain is provided but no client cert is passed" do
    expect {
      Kafka::SslContext.build(client_cert_chain: "HI", client_cert_key: "hello", client_cert: nil)
    }.to raise_exception(ArgumentError)
  end

  it "raises ArgumentError if a client cert chain is provided but no client key is passed" do
    expect {
      Kafka::SslContext.build(client_cert_chain: "HI", client_cert_key: nil, client_cert: "hello")
    }.to raise_exception(ArgumentError)
  end

  context "with self signed cert fixtures" do
    # How the certificates were generated, they are not actually in a chain
    # openssl req -newkey rsa:2048 -nodes -keyout spec/fixtures/client_cert_key.pem -x509 -days 365 -out spec/fixtures/client_cert.pem
    # openssl req -newkey rsa:2048 -nodes -keyout /tmp/key1.pem -x509 -days 365 -out spec/fixtures/client_cert_chain_1.pem
    # openssl req -newkey rsa:2048 -nodes -keyout /tmp/key2.pem -x509 -days 365 -out spec/fixtures/client_cert_chain_2.pem
    # This test should continue to run if the certs are expired unless more validation is added to the ssl_context code, at
    # which point, this spec file should be updated to test the new code.
    let(:client_cert) { IO.read("spec/fixtures/client_cert.pem") }
    let(:client_cert_key) { IO.read("spec/fixtures/client_cert_key.pem") }
    let(:chain1) { IO.read("spec/fixtures/client_cert_chain_1.pem") }
    let(:chain2) { IO.read("spec/fixtures/client_cert_chain_1.pem") }
    let(:combined_chain) { "#{chain1}#{chain2}" }

    subject { Kafka::SslContext.build(client_cert_chain: combined_chain, client_cert: client_cert, client_cert_key: client_cert_key) }

    it "splits the client certificate chain in the proper order" do
      expected_chain = [
        OpenSSL::X509::Certificate.new(chain1),
        OpenSSL::X509::Certificate.new(chain2),
      ]
      expect(subject.extra_chain_cert).to_not be_empty
      expect(subject.extra_chain_cert).to eq expected_chain
    end
  end
end
