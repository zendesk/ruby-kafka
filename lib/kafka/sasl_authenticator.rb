# frozen_string_literal: true

require 'kafka/sasl/plain'
require 'kafka/sasl/gssapi'
require 'kafka/sasl/scram'
require 'kafka/sasl/oauth'
require 'kafka/sasl/awsmskiam'

module Kafka
  class SaslAuthenticator
    def initialize(logger:, sasl_gssapi_principal:, sasl_gssapi_keytab:,
                   sasl_plain_authzid:, sasl_plain_username:, sasl_plain_password:,
                   sasl_scram_username:, sasl_scram_password:, sasl_scram_mechanism:,
                   sasl_oauth_token_provider:,
                   sasl_aws_msk_iam_access_key_id:,
                   sasl_aws_msk_iam_secret_key_id:,
                   sasl_aws_msk_iam_aws_region:,
                   sasl_aws_msk_iam_session_token: nil)
      @logger = TaggedLogger.new(logger)

      @plain = Sasl::Plain.new(
        authzid: sasl_plain_authzid,
        username: sasl_plain_username,
        password: sasl_plain_password,
        logger: @logger,
      )

      @gssapi = Sasl::Gssapi.new(
        principal: sasl_gssapi_principal,
        keytab: sasl_gssapi_keytab,
        logger: @logger,
      )

      @scram = Sasl::Scram.new(
        username: sasl_scram_username,
        password: sasl_scram_password,
        mechanism: sasl_scram_mechanism,
        logger: @logger,
      )

      @aws_msk_iam = Sasl::AwsMskIam.new(
        access_key_id: sasl_aws_msk_iam_access_key_id,
        secret_key_id: sasl_aws_msk_iam_secret_key_id,
        aws_region: sasl_aws_msk_iam_aws_region,
        session_token: sasl_aws_msk_iam_session_token,
        logger: @logger,
      )

      @oauth = Sasl::OAuth.new(
        token_provider: sasl_oauth_token_provider,
        logger: @logger,
      )

      @mechanism = [@gssapi, @plain, @scram, @oauth, @aws_msk_iam].find(&:configured?)
    end

    def enabled?
      !@mechanism.nil?
    end

    def authenticate!(connection)
      return unless enabled?

      ident = @mechanism.ident
      response = connection.send_request(Kafka::Protocol::SaslHandshakeRequest.new(ident))

      unless response.error_code == 0 && response.enabled_mechanisms.include?(ident)
        raise Kafka::Error, "#{ident} is not supported."
      end

      @mechanism.authenticate!(connection.to_s, connection.encoder, connection.decoder)
    end
  end
end
