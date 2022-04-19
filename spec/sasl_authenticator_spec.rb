# frozen_string_literal: true

require 'fake_server'
require 'fake_token_provider'

describe Kafka::SaslAuthenticator do
  let(:logger) { LOGGER }

  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }

  let(:connection) {
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test",
      logger: logger,
      instrumenter: Kafka::Instrumenter.new(client_id: "test"),
      connect_timeout: 0.1,
      socket_timeout: 0.1,
    )
  }

  let!(:fake_server) { FakeServer.start(server) }

  let(:sasl_authenticator) {
    Kafka::SaslAuthenticator.new(
      **{ logger: logger }.merge(auth_options)
    )
  }

  let(:auth_options) {
    {
      sasl_gssapi_principal: nil,
      sasl_gssapi_keytab: nil,
      sasl_plain_authzid: nil,
      sasl_plain_username: nil,
      sasl_plain_password: nil,
      sasl_scram_username: nil,
      sasl_scram_password: nil,
      sasl_scram_mechanism: nil,
      sasl_oauth_token_provider: nil,
      sasl_aws_msk_iam_access_key_id: nil,
      sasl_aws_msk_iam_secret_key_id: nil,
      sasl_aws_msk_iam_aws_region: nil
    }
  }

  context "when SASL has not been configured" do
    it "still works" do
      sasl_authenticator.authenticate!(connection)
    end
  end

  context "when SASL PLAIN has been configured" do
    before do
      auth_options.update(
        sasl_plain_authzid: "",
        sasl_plain_username: "spec_username",
        sasl_plain_password: "spec_password",
      )
    end

    it "authenticates" do
      sasl_authenticator.authenticate!(connection)
    end

    it "raises Kafka::Error when the username or password is incorrect" do
      auth_options[:sasl_plain_password] = "wrong"

      expect {
        sasl_authenticator.authenticate!(connection)
      }.to raise_error(Kafka::Error, /SASL PLAIN authentication failed/)
    end
  end

  context "when SASL SCRAM has been configured" do
    before do
      auth_options.update(
        sasl_scram_username: "spec_username",
        sasl_scram_password: "spec_password",
        sasl_scram_mechanism: "sha256"
      )
    end

    it "authenticates" do
      sasl_authenticator.authenticate!(connection)
    end

    it "raises Kafka::Error when the username or password is incorrect" do
      auth_options[:sasl_scram_password] = "wrong"

      expect {
        sasl_authenticator.authenticate!(connection)
      }.to raise_error(Kafka::FailedScramAuthentication)
    end
  end

  context "when SASL OAuthBearer has been configured" do
    before do
      auth_options.update(
        sasl_oauth_token_provider: FakeTokenProvider.new
      )
    end

    it "authenticates" do
      sasl_authenticator.authenticate!(connection)
    end

    it "authenticates without extensions implemented" do
      auth_options[:sasl_oauth_token_provider] = FakeTokenProviderNoExtensions.new

      sasl_authenticator.authenticate!(connection)
    end

    it "raises error when the token provider does not generate a token" do
      auth_options[:sasl_oauth_token_provider] = FakeBrokenTokenProvider.new

      expect {
        sasl_authenticator.authenticate!(connection)
      }.to raise_error(Kafka::TokenMethodNotImplementedError)
    end
  end
end
