# frozen_string_literal: true

require 'securerandom'
require 'base64'

module Kafka
  module Sasl
    class Scram
      MECHANISMS = {
        "sha256" => "SCRAM-SHA-256",
        "sha512" => "SCRAM-SHA-512",
      }.freeze

      def initialize(username:, password:, mechanism: 'sha256', logger:)
        @semaphore = Mutex.new
        @username = username
        @password = password
        @logger = TaggedLogger.new(logger)

        if mechanism
          @mechanism = MECHANISMS.fetch(mechanism) do
            raise Kafka::SaslScramError, "SCRAM mechanism #{mechanism} is not supported."
          end
        end
      end

      def ident
        @mechanism
      end

      def configured?
        @username && @password && @mechanism
      end

      def authenticate!(host, encoder, decoder)
        @logger.debug "Authenticating #{@username} with SASL #{@mechanism}"

        begin
          @semaphore.synchronize do
            msg = first_message
            @logger.debug "Sending first client SASL SCRAM message: #{msg}"
            encoder.write_bytes(msg)

            @server_first_message = decoder.bytes
            @logger.debug "Received first server SASL SCRAM message: #{@server_first_message}"

            msg = final_message
            @logger.debug "Sending final client SASL SCRAM message: #{msg}"
            encoder.write_bytes(msg)

            response = parse_response(decoder.bytes)
            @logger.debug "Received last server SASL SCRAM message: #{response}"

            raise FailedScramAuthentication, response['e'] if response['e']
            raise FailedScramAuthentication, "Invalid server signature" if response['v'] != server_signature
          end
        rescue EOFError => e
          raise FailedScramAuthentication, e.message
        end

        @logger.debug "SASL SCRAM authentication successful"
      end

      private

      def first_message
        "n,,#{first_message_bare}"
      end

      def first_message_bare
        "n=#{encoded_username},r=#{nonce}"
      end

      def final_message_without_proof
        "c=biws,r=#{rnonce}"
      end

      def final_message
        "#{final_message_without_proof},p=#{client_proof}"
      end

      def server_data
        parse_response(@server_first_message)
      end

      def rnonce
        server_data['r']
      end

      def salt
        Base64.strict_decode64(server_data['s'])
      end

      def iterations
        server_data['i'].to_i
      end

      def auth_message
        [first_message_bare, @server_first_message, final_message_without_proof].join(',')
      end

      def salted_password
        hi(@password, salt, iterations)
      end

      def client_key
        hmac(salted_password, 'Client Key')
      end

      def stored_key
        h(client_key)
      end

      def server_key
        hmac(salted_password, 'Server Key')
      end

      def client_signature
        hmac(stored_key, auth_message)
      end

      def server_signature
        Base64.strict_encode64(hmac(server_key, auth_message))
      end

      def client_proof
        Base64.strict_encode64(xor(client_key, client_signature))
      end

      def h(str)
        digest.digest(str)
      end

      def hi(str, salt, iterations)
        OpenSSL::PKCS5.pbkdf2_hmac(
          str,
          salt,
          iterations,
          digest.size,
          digest
        )
      end

      def hmac(data, key)
        OpenSSL::HMAC.digest(digest, data, key)
      end

      def xor(first, second)
        first.bytes.zip(second.bytes).map { |(a, b)| (a ^ b).chr }.join('')
      end

      def parse_response(data)
        data.split(',').map { |s| s.split('=', 2) }.to_h
      end

      def encoded_username
        safe_str(@username.encode(Encoding::UTF_8))
      end

      def nonce
        @nonce ||= SecureRandom.urlsafe_base64(32)
      end

      def digest
        @digest ||= case @mechanism
                    when 'SCRAM-SHA-256'
                      OpenSSL::Digest::SHA256.new
                    when 'SCRAM-SHA-512'
                      OpenSSL::Digest::SHA512.new
                    else
                      raise ArgumentError, "Unknown SASL mechanism '#{@mechanism}'"
                    end
      end

      def safe_str(val)
        val.gsub('=', '=3D').gsub(',', '=2C')
      end
    end
  end
end
