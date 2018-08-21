# frozen_string_literal: true

module Kafka
  module Protocol
    class AddPartitionsToTxnRequest
      def initialize(transactional_id: nil, producer_id:, producer_epoch:, topics:)
        @transactional_id = transactional_id
        @producer_id = producer_id
        @producer_epoch = producer_epoch
        @topics = topics
      end

      def api_key
        ADD_PARTITIONS_TO_TXN_API
      end

      def response_class
        AddPartitionsToTxnResponse
      end

      def encode(encoder)
        encoder.write_string(@transactional_id.to_s)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)
        encoder.write_array(@topics.to_a) do |topic, partitions|
          encoder.write_string(topic)
          encoder.write_array(partitions) do |partition|
            encoder.write_int32(partition)
          end
        end
      end
    end
  end
end
