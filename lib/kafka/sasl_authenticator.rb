require 'kafka/sasl_gssapi_authenticator'
require 'kafka/sasl_plain_authenticator'
require 'kafka/sasl_scram_authenticator'

module Kafka
  class SaslAuthenticator
    def initialize(logger:, sasl_gssapi_principal:, sasl_gssapi_keytab:,
                   sasl_plain_authzid:, sasl_plain_username:, sasl_plain_password:,
                   sasl_scram_username:, sasl_scram_password:, sasl_scram_mechanism:)
      @logger = logger
      @sasl_gssapi_principal = sasl_gssapi_principal
      @sasl_gssapi_keytab = sasl_gssapi_keytab
      @sasl_plain_authzid = sasl_plain_authzid
      @sasl_plain_username = sasl_plain_username
      @sasl_plain_password = sasl_plain_password
      @sasl_scram_username = sasl_scram_username
      @sasl_scram_password = sasl_scram_password
      @sasl_scram_mechanism = sasl_scram_mechanism
    end

    def authenticate!(connection)
      if authenticate_using_sasl_gssapi?
        sasl_gssapi_authenticate(connection)
      elsif authenticate_using_sasl_plain?
        sasl_plain_authenticate(connection)
      elsif authenticate_using_sasl_scram?
        sasl_scram_authenticate(connection)
      end
    end

    private

    def sasl_scram_authenticate(connection)
      auth = SaslScramAuthenticator.new(
        @sasl_scram_username,
        @sasl_scram_password,
        logger: @logger,
        mechanism: @sasl_scram_mechanism,
        connection: connection
      )

      auth.authenticate!
    end

    def sasl_gssapi_authenticate(connection)
      auth = SaslGssapiAuthenticator.new(
        logger: @logger,
        sasl_gssapi_principal: @sasl_gssapi_principal,
        sasl_gssapi_keytab: @sasl_gssapi_keytab,
        connection: connection
      )

      auth.authenticate!
    end

    def sasl_plain_authenticate(connection)
      auth = SaslPlainAuthenticator.new(
        logger: @logger,
        authzid: @sasl_plain_authzid,
        username: @sasl_plain_username,
        password: @sasl_plain_password,
        connection: connection
      )

      auth.authenticate!
    end

    def authenticate_using_sasl_scram?
      @sasl_scram_username && @sasl_scram_password
    end

    def authenticate_using_sasl_gssapi?
      !@ssl_context && @sasl_gssapi_principal && !@sasl_gssapi_principal.empty?
    end

    def authenticate_using_sasl_plain?
      @sasl_plain_authzid && @sasl_plain_username && @sasl_plain_password
    end
  end
end
