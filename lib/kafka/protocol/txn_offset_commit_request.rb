# frozen_string_literal: true

module Kafka
  module Protocol
    class TxnOffsetCommitRequest

      def api_key
        TXN_OFFSET_COMMIT_API
      end

      def api_version
        2
      end

      def response_class
        TxnOffsetCommitResponse
      end

      def initialize(transactional_id:, group_id:, producer_id:, producer_epoch:, offsets:)
        @transactional_id = transactional_id
        @producer_id = producer_id
        @producer_epoch = producer_epoch
        @group_id = group_id
        @offsets = offsets
      end

      def encode(encoder)
        encoder.write_string(@transactional_id.to_s)
        encoder.write_string(@group_id)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)

        encoder.write_array(@offsets) do |topic, partitions|
          encoder.write_string(topic)
          encoder.write_array(partitions) do |partition, offset|
            encoder.write_int32(partition)
            encoder.write_int64(offset[:offset])
            encoder.write_string(nil) # metadata
            encoder.write_int32(offset[:leader_epoch])
          end
        end
      end

    end
  end
end
