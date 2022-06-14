# frozen_string_literal: true

require 'fake_server'
require 'fake_token_provider'

describe Kafka::Sasl::AwsMskIamCredentials do
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

  let(:sasl_aws_iam_credentials) {
    Kafka::Sasl::AwsMskIamCredentials.new(
      **{ logger: logger }.merge(aws_iam_auth_options)
    )
  }

  let(:aws_iam_auth_options) {
    {
      access_key_id: nil,
      secret_key_id: nil,
      session_token: nil,
      assume_role_credentials: nil
    }
  }

  context "when use access_key_id and secret_key_id" do
    it "raises Kafka::Sasl::AwsMskIamCredentialsException when assume_role_credentials, sasl_aws_msk_iam_secret_key_id and assume_role_credentials are nil" do
      expect {
        sasl_aws_iam_credentials.verify_params
      }.to raise_error(Kafka::Sasl::AwsMskIamCredentialsException)
    end


    it "should not throw Kafka::Sasl::AwsMskIamCredentialsException when both access_key_id and secret_key_id are not nil" do
      aws_iam_auth_options.update(
        access_key_id: "access_key_id",
        secret_key_id: "secret_key_id",
      )
      expect {
        sasl_aws_iam_credentials.verify_params
      }.not_to raise_error(Kafka::Sasl::AwsMskIamCredentialsException)
    end

    it "should get credential value from access_key_id and secret_key_id" do
      aws_iam_auth_options.update(
        access_key_id: "access_key_id",
        secret_key_id: "secret_key_id",
        session_token: "session_token"
      )
      expect(sasl_aws_iam_credentials.get_access_key_id).to eq("access_key_id")
      expect(sasl_aws_iam_credentials.get_secret_key_id).to eq("secret_key_id")
      expect(sasl_aws_iam_credentials.get_session_token).to eq("session_token")
    end
  end


  context "when use assume_role_credentials" do
    let(:mock_assume_role_credentials_obj){
      MockAssumeRoleCredentials.new(
          credentials = MockAWSCredentials.new(
          "mock_assume_role_access_key_id",
          "mock_assume_role_secret_access_key",
          "mock_assume_role_session_token")
      )
    }

    before do
      # in production, one will use AssumeRoleCredentials object(https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/AssumeRoleCredentials.html)
      # the implementation of mock classes are very simple and hacky
      class MockAWSCredentials
        attr_accessor :access_key_id, :secret_access_key, :session_token

        def initialize(access_key_id, secret_access_key, session_token)
          @access_key_id = access_key_id
          @secret_access_key = secret_access_key
          @session_token = session_token
        end
      end

      class MockAssumeRoleCredentials
        attr_reader :credentials

        def initialize(credentials)
          @credentials = credentials
        end
      end
      aws_iam_auth_options.update(
          assume_role_credentials: mock_assume_role_credentials_obj
      )
    end

    it "should get credential value from assume_role_credentials object" do
      expect(sasl_aws_iam_credentials.get_access_key_id).to eq("mock_assume_role_access_key_id")
      expect(sasl_aws_iam_credentials.get_secret_key_id).to eq("mock_assume_role_secret_access_key")
      expect(sasl_aws_iam_credentials.get_session_token).to eq("mock_assume_role_session_token")
    end

    it "should have credentials updated automatically when the credentials object get updated" do
      mock_assume_role_credentials_obj.credentials.access_key_id = "mock_assume_role_access_key_id-updated"
      mock_assume_role_credentials_obj.credentials.secret_access_key = "mock_assume_role_secret_access_key-updated"
      mock_assume_role_credentials_obj.credentials.session_token = "mock_assume_role_session_token-updated"

      expect(sasl_aws_iam_credentials.get_access_key_id).to eq("mock_assume_role_access_key_id-updated")
      expect(sasl_aws_iam_credentials.get_secret_key_id).to eq("mock_assume_role_secret_access_key-updated")
      expect(sasl_aws_iam_credentials.get_session_token).to eq("mock_assume_role_session_token-updated")
    end
  end
end
