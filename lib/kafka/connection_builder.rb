require 'kafka/sasl_gssapi_authenticator'
require 'kafka/sasl_plain_authenticator'

module Kafka
  class ConnectionBuilder
    def initialize(client_id:, logger:, instrumenter:, connect_timeout:, socket_timeout:, ssl_context:, sasl_gssapi_principal:, sasl_gssapi_keytab:, sasl_plain_authzid:, sasl_plain_username:, sasl_plain_password:)
      @client_id = client_id
      @logger = logger
      @instrumenter = instrumenter
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @ssl_context = ssl_context
      @sasl_gssapi_principal = sasl_gssapi_principal
      @sasl_gssapi_keytab = sasl_gssapi_keytab
      @sasl_plain_authzid = sasl_plain_authzid
      @sasl_plain_username = sasl_plain_username
      @sasl_plain_password = sasl_plain_password
    end

    def build_connection(host, port)
      connection = Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
        instrumenter: @instrumenter,
        ssl_context: @ssl_context
      )

      if authenticate_using_sasl_gssapi?
        sasl_gssapi_authenticate(connection)
      elsif authenticate_using_sasl_plain?
        sasl_plain_authenticate(connection)
      end

      connection
    end

    private

    def sasl_gssapi_authenticate(connection)
      auth = SaslGssapiAuthenticator.new(
        connection: connection,
        logger: @logger,
        sasl_gssapi_principal: @sasl_gssapi_principal,
        sasl_gssapi_keytab: @sasl_gssapi_keytab
      )

      auth.authenticate!
    end

    def sasl_plain_authenticate(connection)
      auth = SaslPlainAuthenticator.new(
        connection: connection,
        logger: @logger,
        authzid: @sasl_plain_authzid,
        username: @sasl_plain_username,
        password: @sasl_plain_password
      )

      auth.authenticate!
    end

    def authenticate_using_sasl_gssapi?
      !@ssl_context && @sasl_gssapi_principal && !@sasl_gssapi_principal.empty?
    end

    def authenticate_using_sasl_plain?
      @sasl_plain_authzid && @sasl_plain_username && @sasl_plain_password
    end
  end
end
