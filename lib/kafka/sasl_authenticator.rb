require 'kafka/sasl_gssapi_authenticator'
require 'kafka/sasl_plain_authenticator'

module Kafka
  class SaslAuthenticator
    def initialize(logger:, sasl_gssapi_principal:, sasl_gssapi_keytab:, sasl_plain_authzid:, sasl_plain_username:, sasl_plain_password:)
      @logger = logger
      @sasl_gssapi_principal = sasl_gssapi_principal
      @sasl_gssapi_keytab = sasl_gssapi_keytab
      @sasl_plain_authzid = sasl_plain_authzid
      @sasl_plain_username = sasl_plain_username
      @sasl_plain_password = sasl_plain_password
    end

    def authenticate!(connection)
      if authenticate_using_sasl_gssapi?
        sasl_gssapi_authenticate(connection)
      elsif authenticate_using_sasl_plain?
        sasl_plain_authenticate(connection)
      end
    end

    private

    def sasl_gssapi_authenticate(connection)
      auth = SaslGssapiAuthenticator.new(
        logger: @logger,
        sasl_gssapi_principal: @sasl_gssapi_principal,
        sasl_gssapi_keytab: @sasl_gssapi_keytab
      )

      auth.authenticate!(connection)
    end

    def sasl_plain_authenticate(connection)
      auth = SaslPlainAuthenticator.new(
        logger: @logger,
        authzid: @sasl_plain_authzid,
        username: @sasl_plain_username,
        password: @sasl_plain_password
      )

      auth.authenticate!(connection)
    end

    def authenticate_using_sasl_gssapi?
      !@ssl_context && @sasl_gssapi_principal && !@sasl_gssapi_principal.empty?
    end

    def authenticate_using_sasl_plain?
      @sasl_plain_authzid && @sasl_plain_username && @sasl_plain_password
    end
  end
end
