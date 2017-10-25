module Kafka
  class SaslPlainAuthenticator
    PLAIN_IDENT = "PLAIN"

    def initialize(logger:, authzid:, username:, password:)
      @logger = logger
      @authzid = authzid
      @username = username
      @password = password
    end

    def authenticate!(connection)
      @logger.debug 'Authenticating SASL PLAIN'
      response = connection.send_request(Kafka::Protocol::SaslHandshakeRequest.new(PLAIN_IDENT))

      @encoder = connection.encoder
      @decoder = connection.decoder

      unless response.error_code == 0 && response.enabled_mechanisms.include?(PLAIN_IDENT)
        raise Kafka::Error, "#{PLAIN_IDENT} is not supported."
      end

      # SASL PLAIN
      msg = [@authzid.to_s,
             @username.to_s,
             @password.to_s].join("\000").force_encoding('utf-8')
      @encoder.write_bytes(msg)
      begin
        msg = @decoder.bytes
        raise Kafka::Error, 'SASL PLAIN authentication failed: unknown error' unless msg
      rescue Errno::ETIMEDOUT, EOFError => e
        raise Kafka::Error, "SASL PLAIN authentication failed: #{e.message}"
      end
      @logger.debug 'SASL PLAIN authentication successful.'
    end
  end
end
