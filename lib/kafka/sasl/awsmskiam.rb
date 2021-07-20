# frozen_string_literal: true

require 'securerandom'
require 'base64'
require 'json'

module Kafka
  module Sasl
    class AwsMskIam
      AWS_MSK_IAM = "AWS_MSK_IAM"

      def initialize(aws_region:, access_key_id:, secret_key_id:, logger:)
        @semaphore = Mutex.new

        @aws_region = aws_region
        @access_key_id = access_key_id
        @secret_key_id = secret_key_id
        @logger = TaggedLogger.new(logger)
      end

      def ident
        AWS_MSK_IAM
      end

      def configured?
        @aws_region && @access_key_id && @secret_key_id
      end

      def authenticate!(host, encoder, decoder)
        @logger.debug "Authenticating #{@access_key_id} with SASL #{AWS_MSK_IAM}"

        host_without_port = host.split(':', -1).first

        msg = authentication_payload(host: host_without_port)
        @logger.debug "Sending first client SASL AWS_MSK_IAM message:"
        @logger.debug msg
        encoder.write_bytes(msg)

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

      def aws_uri_encode(str, encode_slash = true)
        result = ''
        chars = str.split('')
        chars.each do |char|
          if ('A'..'Z').include?(char) || ('a'..'z').include?(char) || ('0'..'9').include?(char) || char == '_' || char == '-' || char == '~' || char == '.'
            result += char
          elsif char == '/'
            if encode_slash
              result += '%2F'
            else
              result += char
            end
          else
            result += '%' + bin_to_hex(char).upcase
          end
        end

        result
      end

      def bin_to_hex(s)
        s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
      end

      def digest
        @digest ||= OpenSSL::Digest::SHA256.new
      end

      def authentication_payload(host:)
        now = Time.now
        {
          'version': "2020_10_22",
          'host': host,
          'user-agent': "ruby-kafka",
          'action': "kafka-cluster:Connect",
          'x-amz-algorithm': "AWS4-HMAC-SHA256",
          'x-amz-credential': @access_key_id + "/" + now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request",
          'x-amz-date': now.strftime("%Y%m%dT%H%M%SZ"),
          'x-amz-signedheaders': "host",
          'x-amz-expires': "900",
          'x-amz-signature': signature(host: host)
        }.to_json
      end

      def canonical_request(host:)
        "GET\n" +
        "/\n" +
        canonical_query_string + "\n" +
        canonical_headers(host: host) + "\n" +
        signed_headers + "\n" +
        hashed_payload
      end

      def canonical_query_string
        now = Time.now
        aws_uri_encode("Action") + "=" + aws_uri_encode("kafka-cluster:Connect") + "&" +
        aws_uri_encode("X-Amz-Algorithm") + "=" + aws_uri_encode("AWS4-HMAC-SHA256") + "&" +
        aws_uri_encode("X-Amz-Credential") + "=" + aws_uri_encode(@access_key_id + "/" + now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request") + "&" +
        aws_uri_encode("X-Amz-Date") + "=" + aws_uri_encode(now.strftime("%Y%m%dT%H%M%SZ")) + "&" +
        aws_uri_encode("X-Amz-Expires") + "=" + aws_uri_encode("900") + "&" +
        aws_uri_encode("X-Amz-SignedHeaders") + "=" + aws_uri_encode("host")
      end

      def canonical_headers(host:)
        "host" + ":" + host + "\n"
      end

      def signed_headers
        "host"
      end

      def hashed_payload
        bin_to_hex(digest.digest(""))
      end

      def string_to_sign(host:)
        now = Time.now
        "AWS4-HMAC-SHA256" + "\n" +
        now.strftime("%Y%m%dT%H%M%SZ") + "\n" +
        now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request" + "\n" +
        bin_to_hex(digest.digest(canonical_request(host: host)))
      end

      def signature(host:)
        now = Time.now

        date_key = OpenSSL::HMAC.digest("SHA256", "AWS4" + @secret_key_id, now.strftime("%Y%m%d"))
        date_region_key = OpenSSL::HMAC.digest("SHA256", date_key, @aws_region)
        date_region_service_key = OpenSSL::HMAC.digest("SHA256", date_region_key, "kafka-cluster")
        signing_key = OpenSSL::HMAC.digest("SHA256", date_region_service_key, "aws4_request")
        signature = bin_to_hex(OpenSSL::HMAC.digest("SHA256", signing_key, string_to_sign(host: host)))

        signature
      end
    end
  end
end
