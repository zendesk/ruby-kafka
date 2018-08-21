# frozen_string_literal: true

module Kafka
  module Protocol
    class InitProducerIDRequest
      def initialize(transactional_id: nil, transactional_timeout:)
        @transactional_id = transactional_id
        @transactional_timeout = transactional_timeout
      end

      def api_key
        INIT_PRODUCER_ID_API
      end

      def response_class
        InitProducerIDResponse
      end

      def encode(encoder)
        encoder.write_string(@transactional_id)
        # Timeout is in ms unit
        encoder.write_int32(@transactional_timeout * 1000)
      end
    end
  end
end
