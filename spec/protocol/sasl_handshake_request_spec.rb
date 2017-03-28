require 'kafka/protocol/api_key'

describe Kafka::Protocol::SaslHandshakeRequest do
  describe "#api_key" do
    let(:request) { Kafka::Protocol::SaslHandshakeRequest.new('GSSAPI') }
    it 'expects correct api_key' do
      expect(request.api_key).to eq Kafka::Protocol::ApiKey::SASL_HANDSHAKE
    end
  end

  describe "#initialize" do
    context "#supported" do
      it "allows GSSAPI" do
        expect { Kafka::Protocol::SaslHandshakeRequest.new('GSSAPI') }.not_to raise_error
      end
    end
    context "#unsupported" do
      it "reject unknown handshake" do
        expect { Kafka::Protocol::SaslHandshakeRequest.new('Unsupported') }.to raise_error Kafka::Error
      end
    end
  end
end
