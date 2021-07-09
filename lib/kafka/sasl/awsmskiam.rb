# frozen_string_literal: true

require 'securerandom'
require 'base64'

module Kafka
  module Sasl
    class AwsMskIam
      AWS_MSK_IAM = "AWS_MSK_IAM"

      def initialize(host:, aws_region:, access_key_id:, secret_key_id:, logger:)
        @semaphore = Mutex.new

        @host = host
        @aws_region = aws_region
        @access_key_id = access_key_id
        @secret_key_id = secret_key_id
        @logger = TaggedLogger.new(logger)
      end

      def ident
        AWS_MSK_IAM
      end

      def configured?
        @host && @aws_region && @access_key_id && @secret_key_id
      end

      def authenticate!(host, encoder, decoder)
        @logger.debug "Authenticating #{@access_key_id} with SASL #{AWS_MSK_IAM}"

        msg = authentication_payload
        @logger.debug "Sending first client SASL AWS_MSK_IAM message: #{msg}"
        encoder.write_bytes(msg)

        @server_first_message = decoder.bytes
        @logger.debug "Received first server SASL AWS_MSK_IAM message: #{@server_first_message}"

        begin
          @server_first_message = decoder.bytes
          @logger.debug "Received first server SASL AWS_MSK_IAM message: #{@server_first_message}"

          raise Kafka::Error, "SASL AWS_MSK_IAM authentication failed: unknown error" unless @server_first_message
        rescue Errno::ETIMEDOUT, EOFError => e
          raise Kafka::Error, "SASL AWS_MSK_IAM authentication failed: #{e.message}"
        end

        @logger.debug "SASL #{AWS_MSK_IAM} authentication successful"
      end

      private

      def authentication_payload
        now = Time.now
        {
          'version': "2020_10_22",
          'host': @host,
          'user-agent': "ruby-kafka",
          'action': "kafka-cluster:Connect",
          'x-amz-algorithm': "AWS4-HMAC-SHA256",
          'x-amz-credential': @access_key_id + "/" + now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request",
          'x-amz-date': now.strftime("%Y%m%dT%H%M%SZ"),
          'x-amz-signedheaders': "host",
          'x-amz-expires': "900",
          'x-amz-signature': signature
        }
      end

      def canonical_request
        "GET\n" +
        "/\n" +
        canonical_query_string + "\n"+
        canonical_headers + "\n"+
        signed_headers + "\n"
        hashed_payload
      end

      def canonical_query_string
        now = Time.now
        CGI.escape("Action") + "=" + CGI.escape("kafka-cluster:Connect") + "&" +
        CGI.escape("X-Amz-Algorithm") + "=" + CGI.escape("AWS4-HMAC-SHA256") + "&" +
        CGI.escape("X-Amz-Credential") + "=" + CGI.escape(@access_key_id) + "/" + now.strftime("%Y%m%d") + "/" + CGI.escape(@aws_region + "/kafka-cluster/aws4_request") + "&" +
        CGI.escape("X-Amz-Date") + "=" + CGI.escape(now.strftime("%Y%m%dT%H%M%SZ")) + "&" +
        CGI.escape("X-Amz-Expires") + "=" + CGI.escape("900") + "&" +
        CGI.escape("X-Amz-Security-Token") + "=" + CGI.escape("<client Session Token>") + "&" +
        CGI.escape("X-Amz-SignedHeaders") + "=" + CGI.escape("host")
      end

      def canonical_headers
        "host" + ":" + @host + "\n"
      end

      def signed_headers
        "host"
      end

      def hashed_payload
        bin_to_hex(digest.digest(""))
      end

      def string_to_sign
        now = Time.now
        "AWS4-HMAC-SHA256" + "\n" +
        now.strftime("%Y%m%dT%H%M%SZ") + "\n" +
        @aws_region + "/kafka-cluster" + "\n" +
        bin_to_hex(digest.digest(canonical_request))
      end

      def signature
        now = Time.now

        date_key = OpenSSL::HMAC.digest("SHA256", "AWS4" + @secret_key_id, now.strftime("%Y%m%d"))
        date_region_key = OpenSSL::HMAC.digest("SHA256", date_key, @aws_region)
        date_region_service_key = OpenSSL::HMAC.digest("SHA256", date_region_key, "kafka-cluster")
        signing_key = OpenSSL::HMAC.digest("SHA256", signing_key, "aws4_request")
        signature = bin_to_hex(OpenSSL::HMAC.digest("SHA256", signing_key, string_to_sign))
      end

      def bin_to_hex(s)
        s.each_byte.map { |b| b.to_s(16) }.join
      end

      def digest
        @digest ||= OpenSSL::Digest::SHA256.new
      end
    end
  end
end
