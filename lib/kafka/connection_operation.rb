module Kafka
  class ConnectionOperation
    def initialize(logger:, client_id:, encoder:, decoder:)
      @logger = logger
      @client_id = client_id
      @encoder = encoder
      @decoder = decoder
    end

    # Writes a request over the connection.
    #
    # @param request [#encode] the request that should be encoded and written.
    # @param notification
    # @param correlation_id
    # @return [nil]
    def write_request(request, notification, correlation_id)
      @logger.debug "Sending request #{correlation_id} to #{to_s}"

      message = Kafka::Protocol::RequestMessage.new(
        api_key: request.api_key,
        api_version: request.respond_to?(:api_version) ? request.api_version : 0,
        correlation_id: correlation_id,
        client_id: @client_id,
        request: request,
      )

      data = Kafka::Protocol::Encoder.encode_with(message)
      notification[:request_size] = data.bytesize

      @encoder.write_bytes(data)

      nil
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while writing request #{correlation_id}"
      raise
    end

    # Reads a response from the connection.
    #
    # @param response_class [#decode] an object that can decode the response from
    #   a given Decoder.
    # @param notification
    # @param expected_correlation_id
    # @return [nil]
    def read_response(response_class, notification, expected_correlation_id)
      @logger.debug "Waiting for response #{expected_correlation_id} from #{to_s}"

      data = @decoder.bytes
      notification[:response_size] = data.bytesize

      buffer = StringIO.new(data)
      response_decoder = Kafka::Protocol::Decoder.new(buffer)

      correlation_id = response_decoder.int32
      response = response_class.decode(response_decoder)

      @logger.debug "Received response #{correlation_id} from #{to_s}"

      return correlation_id, response
    rescue Errno::ETIMEDOUT
      @logger.error "Timed out while waiting for response #{expected_correlation_id}"
      raise
    end

    def wait_for_response(response_class, notification, expected_correlation_id)
      loop do
        correlation_id, response = read_response(response_class, notification, expected_correlation_id)

        # There may have been a previous request that timed out before the client
        # was able to read the response. In that case, the response will still be
        # sitting in the socket waiting to be read. If the response we just read
        # was to a previous request, we can safely skip it.
        if correlation_id < expected_correlation_id
          @logger.error "Received out-of-order response id #{correlation_id}, was expecting #{expected_correlation_id}"
        elsif correlation_id > expected_correlation_id
          raise Kafka::Error, "Correlation id mismatch: expected #{expected_correlation_id} but got #{correlation_id}"
        else
          return response
        end
      end
    end
  end
end
