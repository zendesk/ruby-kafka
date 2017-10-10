describe Kafka::Protocol::SaslHandshakeRequest do
  describe "#api_key" do
    let(:request) { Kafka::Protocol::SaslHandshakeRequest.new('GSSAPI') }
    it 'expects correct api_key' do
      expect(request.api_key).to eq 17
    end
  end
end
