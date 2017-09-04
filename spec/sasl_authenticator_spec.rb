require 'fake_server'

class FakeConnection
  attr_reader :encoder, :decoder

  def initialize(encoder, decoder)
    @request_handler = proc {}
    @encoder = encoder
    @decoder = decoder
  end

  def send_request(request)
    @request_handler.call(request)
  end

  def on_request(&block)
    @request_handler = block
  end
end

class FakeSasl
  def initialize

  end
end

describe Kafka::SaslAuthenticator do
  let(:gssapi_principal) { "" }
  let(:gssapi_keytab) { "" }
  let(:plain_authzid) { "" }
  let(:plain_username) { "" }
  let(:plain_password) { "" }

  let(:uplink) { StringIO.new }
  let(:downlink) { StringIO.new }

  let(:logger) { Logger.new(StringIO.new) }

  let(:connection) {
    FakeConnection.new(
      Kafka::Protocol::Encoder.new(uplink),
      Kafka::Protocol::Decoder.new(downlink),
    )
  }

  let(:sasl) {
    described_class.new(
      logger: logger,
      sasl_gssapi_principal: gssapi_principal,
      sasl_gssapi_keytab: gssapi_keytab,
      sasl_plain_authzid: plain_authzid,
      sasl_plain_username: plain_username,
      sasl_plain_password: plain_password,
    )
  }

  context "using PLAIN authentication" do
    it "authenticates a connection" do
      connection.on_request do |request|
        case request
        when Kafka::Protocol::SaslHandshakeRequest
          Kafka::Protocol::SaslHandshakeResponse.new(
            error_code: 0,
            enabled_mechanisms: ["PLAIN"],
          )
        end
      end

      sasl.authenticate!(connection)
    end
  end
end

describe Kafka::SaslAuthenticator do
  let(:logger) { LOGGER }
  let(:connection) { double("Connection") }

  let(:sasl_authenticator) {
    Kafka::SaslAuthenticator.new(
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
        expect{ sasl_authenticator.authenticate!(connection) }.to_not raise_error
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

        sasl_authenticator.authenticate!(connection)
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

        sasl_authenticator.authenticate!(connection)
      end
    end
  end
end
