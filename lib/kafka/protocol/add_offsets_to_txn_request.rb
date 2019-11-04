# frozen_string_literal: true

module Kafka
  module Protocol
    class AddOffsetsToTxnRequest
      def initialize(transactional_id: nil, producer_id:, producer_epoch:, group_id:)
        @transactional_id = transactional_id
        @producer_id = producer_id
        @producer_epoch = producer_epoch
        @group_id = group_id
      end

      def api_key
        ADD_OFFSETS_TO_TXN_API
      end

      def response_class
        AddOffsetsToTxnResponse
      end

      def encode(encoder)
        encoder.write_string(@transactional_id.to_s)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)
        encoder.write_string(@group_id)
      end
    end
  end
end
