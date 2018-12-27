# frozen_string_literal: true

module Kafka
  module Sasl
    class Plain
      PLAIN_IDENT = "PLAIN"

      def initialize(logger:, authzid:, username:, password:)
        @logger = TaggedLogger.new(logger)
        @authzid = authzid
        @username = username
        @password = password
      end

      def ident
        PLAIN_IDENT
      end

      def configured?
        @authzid && @username && @password
      end

      def authenticate!(host, encoder, decoder)
        msg = [@authzid, @username, @password].join("\000").force_encoding("utf-8")

        encoder.write_bytes(msg)

        begin
          msg = decoder.bytes
          raise Kafka::Error, "SASL PLAIN authentication failed: unknown error" unless msg
        rescue Errno::ETIMEDOUT, EOFError => e
          raise Kafka::Error, "SASL PLAIN authentication failed: #{e.message}"
        end

        @logger.debug "SASL PLAIN authentication successful."
      end
    end
  end
end
