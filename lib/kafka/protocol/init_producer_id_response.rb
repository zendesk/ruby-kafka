# frozen_string_literal: true

module Kafka
  module Protocol
    class InitProducerIDResponse
      attr_reader :error_code, :producer_id, :producer_epoch

      def initialize(error_code:, producer_id:, producer_epoch:)
        @error_code = error_code
        @producer_id = producer_id
        @producer_epoch = producer_epoch
      end

      def self.decode(decoder)
        _throttle_time_ms = decoder.int32
        error_code = decoder.int16
        producer_id = decoder.int64
        producer_epoch = decoder.int16
        new(
          error_code: error_code,
          producer_id: producer_id,
          producer_epoch: producer_epoch
        )
      end
    end
  end
end
