require 'fake_server'

describe Kafka::SaslAuthenticator do
  let(:logger) { LOGGER }

  let(:connection) { double("Connection") }

  let(:sasl_authenticator) {
    Kafka::SaslAuthenticator.new(
      { logger: logger }.merge(auth_options)
    )
  }

  describe '#authenticate!' do
    context "when no authentication" do
      let(:auth_options) {
        {
          sasl_gssapi_principal: nil,
          sasl_gssapi_keytab: nil,
          sasl_plain_authzid: nil,
          sasl_plain_username: nil,
          sasl_plain_password: nil,
          sasl_scram_username: nil,
          sasl_scram_password: nil,
          sasl_scram_mechanism: nil
        }
      }

      it "doesn't call any authentication strategy" do
        expect { sasl_authenticator.authenticate!(connection) }.to_not raise_error
      end
    end

    context "when sasl palin authentication" do
      let(:auth_options) {
        {
          sasl_gssapi_principal: nil,
          sasl_gssapi_keytab: nil,
          sasl_plain_authzid: "",
          sasl_plain_username: "user",
          sasl_plain_password: "pass",
          sasl_scram_username: nil,
          sasl_scram_password: nil,
          sasl_scram_mechanism: nil
        }
      }
      let(:auth) { instance_double(Kafka::SaslPlainAuthenticator) }

      it "uses sasl plain authentication strategy" do
        expect(Kafka::SaslPlainAuthenticator).to receive(:new).and_return(auth)
        expect(auth).to receive(:authenticate!)

        sasl_authenticator.authenticate!(connection)
      end
    end

    context "when sasl gssapi authentication" do
      let(:auth_options) {
        {
          sasl_gssapi_principal: "foo",
          sasl_gssapi_keytab: "bar",
          sasl_plain_authzid: "",
          sasl_plain_username: nil,
          sasl_plain_password: nil,
          sasl_scram_username: nil,
          sasl_scram_password: nil,
          sasl_scram_mechanism: nil
        }
      }

      let(:auth) { instance_double(Kafka::SaslGssapiAuthenticator) }

      it "uses sasl gssapi authentication strategy" do
        expect(Kafka::SaslGssapiAuthenticator).to receive(:new).and_return(auth)
        expect(auth).to receive(:authenticate!)

        sasl_authenticator.authenticate!(connection)
      end
    end

    context "when sasl scram authentication" do
      let(:auth_options) {
        {
          sasl_gssapi_principal: nil,
          sasl_gssapi_keytab: nil,
          sasl_plain_authzid: nil,
          sasl_plain_username: nil,
          sasl_plain_password: nil,
          sasl_scram_username: "username",
          sasl_scram_password: "password",
          sasl_scram_mechanism: Kafka::SCRAM_SHA256
        }
      }
      let(:auth) { instance_double(Kafka::SaslScramAuthenticator) }

      it "uses sasl scram authentication strategy" do
        expect(Kafka::SaslScramAuthenticator).to receive(:new).and_return(auth)
        expect(auth).to receive(:authenticate!)

        sasl_authenticator.authenticate!(connection)
      end
    end
  end
end
