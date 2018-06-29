# frozen_string_literal: true

module Kafka
  module Protocol
    class EndTxnRequest
      def initialize(transactional_id:, producer_id:, producer_epoch:, transaction_result:)
        @transactional_id = transactional_id
        @producer_id = producer_id
        @producer_epoch = producer_epoch
        @transaction_result = transaction_result
      end

      def api_key
        END_TXN_API
      end

      def response_class
        EndTxnResposne
      end

      def encode(encoder)
        encoder.write_string(@transactional_id)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)
        encoder.write_boolean(@transaction_result)
      end
    end
  end
end
