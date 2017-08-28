describe Kafka::Sasl do
  describe "#authenticate" do
    before do
      allow(Kafka::SaslGssapiAuthenticator).to receive(:authenticate)
      allow(Kafka::SaslPlainAuthenticator).to receive(:authenticate)
    end

    let(:connection) { double(:connection) }
    let(:logger) { Logger.new(StringIO.new) }
    let(:ssl_context) { nil }
    let(:gssapi_principal) { "" }
    let(:gssapi_keytab) { "" }
    let(:plain_authzid) { "" }
    let(:plain_username) { "" }
    let(:plain_password) { "" }

    let(:sasl) {
      Kafka::Sasl.new(
        logger: logger,
        ssl_context: ssl_context,
        gssapi_principal: gssapi_principal,
        gssapi_keytab: gssapi_keytab,
        plain_authzid: plain_authzid,
        plain_username: plain_username,
        plain_password: plain_password,
      )
    }

    it "tries to authenticate with GSSAPI" do
      sasl.authenticate(connection)
    end
  end
end
