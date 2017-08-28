require 'kafka/sasl_gssapi_authenticator'
require 'kafka/sasl_plain_authenticator'

module Kafka
  class Sasl
    def initialize(logger:, ssl_context:, gssapi_principal:, gssapi_keytab:, plain_authzid:, plain_username:, plain_password:)
      @logger = logger
      @ssl_context = ssl_context
      @gssapi_principal = gssapi_principal
      @gssapi_keytab = gssapi_keytab
      @plain_authzid = plain_authzid
      @plain_username = plain_username
      @plain_password = plain_password
    end

    def authenticate(connection)
      if authenticate_using_gssapi?
        gssapi_authenticate(connection)
      elsif authenticate_using_plain?
        plain_authenticate(connection)
      end
    end

    private

    def gssapi_authenticate(connection)
      SaslGssapiAuthenticator.authenticate(
        connection: connection,
        logger: @logger,
        sasl_gssapi_principal: @sasl_gssapi_principal,
        sasl_gssapi_keytab: @sasl_gssapi_keytab
      )
    end

    def plain_authenticate(connection)
      SaslPlainAuthenticator.authenticate(
        connection: connection,
        logger: @logger,
        authzid: @sasl_plain_authzid,
        username: @sasl_plain_username,
        password: @sasl_plain_password
      )
    end

    def authenticate_using_gssapi?
      !@ssl_context && @gssapi_principal && !@gssapi_principal.empty?
    end

    def authenticate_using_plain?
      @plain_authzid && @plain_username && @plain_password
    end
  end
end
