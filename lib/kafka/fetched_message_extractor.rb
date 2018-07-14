# frozen_string_literal: true

module Kafka
  class FetchedMessageExtractor
    ABORTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x00".freeze

    def initialize(logger:)
      @logger = logger
    end

    def extract(topic, fetched_partition)
      return [] if fetched_partition.messages.empty?

      if fetched_partition.messages.first.is_a?(Kafka::Protocol::MessageSet)
        extract_messages(fetched_partition)
      else
        extract_records(topic, fetched_partition)
      end
    end

    private

    def extract_messages(fetched_partition)
      fetched_partition.messages.flat_map { |message_set| message_set.messages }
    end

    def extract_records(topic, fetched_partition)
      records_by_producer_id = {}

      fetched_partition.messages.each do |record_batch|
        record_batch.records.each do |record|
          if record.is_control_record
            if record.key == ABORTED_TRANSACTION_SIGNAL && !fetched_partition.aborted_transactions.empty?
              reject_aborted_transactions(
                records_by_producer_id, fetched_partition.aborted_transactions
              )
            end
          else
            records_by_producer_id[record_batch.producer_id] ||= []
            records_by_producer_id[record_batch.producer_id] << FetchedMessage.new(
              message: record,
              topic: topic,
              partition: fetched_partition.partition
            )
          end
        end
      end
      records_by_producer_id.values.flatten
    end

    def reject_aborted_transactions(records_by_producer_id, aborted_transactions)
      aborted_transactions.each do |abort_transaction|
        if records_by_producer_id.key?(abort_transaction[:producer_id])
          records_by_producer_id[abort_transaction[:producer_id]].reject! do |record|
            record.offset >= abort_transaction[:first_offset]
          end
        end
      end
    end
  end
end
