require 'fake_server'

describe Kafka::Authenticator do
  let(:logger) { LOGGER }

  let(:connection) { double("Connection") }

  let(:authenticator) {
    Kafka::Authenticator.new(
      {logger: logger}.merge(auth_options)
    )
  }

  describe '#authenticate!' do
    context "when no authentication" do
      let(:auth_options){
        {
          sasl_gssapi_principal: nil,
          sasl_gssapi_keytab: nil,
          sasl_plain_authzid: nil,
          sasl_plain_username: nil,
          sasl_plain_password: nil
        }
      }

      it "doesn't call any authentication strategy" do
        expect{ authenticator.authenticate!(connection) }.to_not raise_error
      end
    end

    context "when sasl palin authentication" do
      let(:auth_options){
        {
          sasl_gssapi_principal: nil,
          sasl_gssapi_keytab: nil,
          sasl_plain_authzid: "",
          sasl_plain_username: "user",
          sasl_plain_password: "pass"
        }
      }
      let(:auth) { instance_double(Kafka::SaslPlainAuthenticator) }

      it "uses sasl plain authentication strategy" do
        expect(Kafka::SaslPlainAuthenticator).to receive(:new).and_return(auth)
        expect(auth).to receive(:authenticate!)

        authenticator.authenticate!(connection)
      end
    end

    context "when sasl gssapi authentication" do
      let(:auth_options){
        {
          sasl_gssapi_principal: "foo",
          sasl_gssapi_keytab: "bar",
          sasl_plain_authzid: "",
          sasl_plain_username: nil,
          sasl_plain_password: nil
        }
      }

      let(:auth) { instance_double(Kafka::SaslGssapiAuthenticator) }

      it "uses sasl gssapi authentication strategy" do
        expect(Kafka::SaslGssapiAuthenticator).to receive(:new).and_return(auth)
        expect(auth).to receive(:authenticate!)

        authenticator.authenticate!(connection)
      end

    end
  end
end
