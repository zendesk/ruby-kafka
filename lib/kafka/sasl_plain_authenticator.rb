module Kafka
  class SaslPlainAuthenticator
    PLAIN_IDENT = "PLAIN"

    def initialize(connection:, logger:, authzid:, username:, password:)
      @connection = connection
      @logger = logger
      @authzid = authzid
      @username = username
      @password = password
    end

    def authenticate!
      response = @connection.send_request(Kafka::Protocol::SaslHandshakeRequest.new(PLAIN_IDENT))

      @encoder = @connection.encoder
      @decoder = @connection.decoder

      unless response.error_code == 0 && response.enabled_mechanisms.include?(PLAIN_IDENT)
        raise Kafka::Error, "#{PLAIN_IDENT} is not supported."
      end

      # SASL PLAIN
      msg = [@authzid.to_s,
             @username.to_s,
             @password.to_s].join("\000").force_encoding("utf-8")
      @encoder.write([msg.bytesize].pack("l>")+msg)
      begin
        msg = @decoder.bytes
        raise Kafka::Error, "SASL PLAIN authentication failed." unless msg
      rescue Errno::ETIMEDOUT
        raise Kafka::Error, "SASL PLAIN authentication failed." unless msg
      end
      @logger.debug "SASL PLAIN authentication successful."
    end
  end
end
