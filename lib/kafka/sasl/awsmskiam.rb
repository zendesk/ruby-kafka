# frozen_string_literal: true

require 'securerandom'
require 'base64'
require 'json'

module Kafka
  module Sasl

    class AwsMskIamCredentialsException < StandardError
      def initialize(msg = "Aws Msk Iam Credentials Exception", exception_type = "Aws Msk Iam Credentials Exception")
        @exception_type = exception_type
        super(msg)
      end
    end

    # AWS IAM Credentials will be encapsulated in an object
    # so when the assume role credential object gets refreshed in the background by AWS SDK,
    # the instance variable @assume_role_credentials will be "refreshed" as well(Ruby is "pass by object reference");
    # if one just tried to retrieve access_key_id and secret_key_id from the temporary credential and pass those values as strings,
    # those strings would not get updated in the background
    class AwsMskIamCredentials
      def initialize(access_key_id: nil, secret_key_id: nil, session_token: nil, assume_role_credentials: nil, logger:)
        @access_key_id = access_key_id
        @secret_key_id = secret_key_id
        @session_token = session_token
        @assume_role_credentials = assume_role_credentials
        @logger = TaggedLogger.new(logger)

        begin
          verify_params
        rescue AwsMskIamCredentialsException
          @logger.error("to create AwsMskIamCredentials, should provide either 1. assume_role_credentials or 2. access_key_id and access_key_id. session_token is optional but can be required depends on the scenario")
        end
      end

      def verify_params
        if @assume_role_credentials.nil? and (@access_key_id.nil? || @secret_key_id.nil?)
          raise AwsMskIamCredentialsException.new(msg = "to create AwsMskIamCredentials, should provide either 1. assume_role_credentials or 2. access_key_id and access_key_id")
        end
      end

      def get_access_key_id
        @assume_role_credentials.nil? ? @access_key_id : @assume_role_credentials.credentials.access_key_id
      end

      def get_secret_key_id
        @assume_role_credentials.nil? ? @secret_key_id : @assume_role_credentials.credentials.secret_access_key
      end

      def get_session_token
        @assume_role_credentials.nil? ? @session_token : @assume_role_credentials.credentials.session_token
      end
    end

    class AwsMskIam
      AWS_MSK_IAM = "AWS_MSK_IAM"

      def initialize(aws_region:, aws_msk_iam_credentials:, logger:)
        @semaphore = Mutex.new

        @aws_region = aws_region
        @aws_msk_iam_credentials = aws_msk_iam_credentials
        @logger = TaggedLogger.new(logger)
      end

      def ident
        AWS_MSK_IAM
      end

      def configured?
        @aws_region && @aws_msk_iam_credentials.get_access_key_id && @aws_msk_iam_credentials.get_secret_key_id
      end

      def authenticate!(host, encoder, decoder)
        @logger.debug "Authenticating #{@aws_msk_iam_credentials.get_access_key_id} with SASL #{AWS_MSK_IAM}"

        host_without_port = host.split(':', -1).first

        time_now = Time.now.utc

        msg = authentication_payload(host: host_without_port, time_now: time_now)
        @logger.debug "Sending first client SASL AWS_MSK_IAM message:"
        @logger.debug msg
        encoder.write_bytes(msg)

        begin
          @server_first_message = decoder.bytes
          @logger.debug "Received first server SASL AWS_MSK_IAM message: #{@server_first_message}"

          raise Kafka::Error, "SASL AWS_MSK_IAM authentication failed: unknown error" unless @server_first_message
        rescue Errno::ETIMEDOUT, EOFError => e
          @logger.error "SASL AWS_MSK_IAM authentication failed: #{e.backtrace.join("\n")}"
          raise Kafka::Error, "SASL AWS_MSK_IAM authentication failed: #{e.message}"
        end

        @logger.debug "SASL #{AWS_MSK_IAM} authentication successful"
      end

      private

      def bin_to_hex(s)
        s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
      end

      def digest
        @digest ||= OpenSSL::Digest::SHA256.new
      end

      def authentication_payload(host:, time_now:)
        {
          'version' => "2020_10_22",
          'host' => host,
          'user-agent' => "ruby-kafka",
          'action' => "kafka-cluster:Connect",
          'x-amz-algorithm' => "AWS4-HMAC-SHA256",
          'x-amz-credential' => @aws_msk_iam_credentials.get_access_key_id + "/" + time_now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request",
          'x-amz-date' => time_now.strftime("%Y%m%dT%H%M%SZ"),
          'x-amz-signedheaders' => "host",
          'x-amz-expires' => "900",
          'x-amz-security-token' => @aws_msk_iam_credentials.get_session_token,
          'x-amz-signature' => signature(host: host, time_now: time_now)
        }.delete_if { |_, v| v.nil? }.to_json
      end

      def canonical_request(host:, time_now:)
        "GET\n" +
          "/\n" +
          canonical_query_string(time_now: time_now) + "\n" +
          canonical_headers(host: host) + "\n" +
          signed_headers + "\n" +
          hashed_payload
      end

      def canonical_query_string(time_now:)
        params = {
          "Action" => "kafka-cluster:Connect",
          "X-Amz-Algorithm" => "AWS4-HMAC-SHA256",
          "X-Amz-Credential" => @aws_msk_iam_credentials.get_access_key_id + "/" + time_now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request",
          "X-Amz-Date" => time_now.strftime("%Y%m%dT%H%M%SZ"),
          "X-Amz-Expires" => "900",
          "X-Amz-Security-Token" => @aws_msk_iam_credentials.get_session_token,
          "X-Amz-SignedHeaders" => "host"
        }.delete_if { |_, v| v.nil? }

        URI.encode_www_form(params)
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

      def string_to_sign(host:, time_now:)
        "AWS4-HMAC-SHA256" + "\n" +
          time_now.strftime("%Y%m%dT%H%M%SZ") + "\n" +
          time_now.strftime("%Y%m%d") + "/" + @aws_region + "/kafka-cluster/aws4_request" + "\n" +
          bin_to_hex(digest.digest(canonical_request(host: host, time_now: time_now)))
      end

      def signature(host:, time_now:)
        date_key = OpenSSL::HMAC.digest("SHA256", "AWS4" + @aws_msk_iam_credentials.get_secret_key_id, time_now.strftime("%Y%m%d"))
        date_region_key = OpenSSL::HMAC.digest("SHA256", date_key, @aws_region)
        date_region_service_key = OpenSSL::HMAC.digest("SHA256", date_region_key, "kafka-cluster")
        signing_key = OpenSSL::HMAC.digest("SHA256", date_region_service_key, "aws4_request")
        signature = bin_to_hex(OpenSSL::HMAC.digest("SHA256", signing_key, string_to_sign(host: host, time_now: time_now)))

        signature
      end
    end
  end
end
