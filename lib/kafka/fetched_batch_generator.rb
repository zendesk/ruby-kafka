# frozen_string_literal: true

require "kafka/fetched_batch"

module Kafka
  class FetchedBatchGenerator
    COMMITTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x01".freeze
    ABORTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x00".freeze

    def initialize(topic, fetched_partition, logger:)
      @topic = topic
      @fetched_partition = fetched_partition
      @logger = logger
    end

    def generate
      messages =
        if @fetched_partition.messages.empty?
          []
        elsif @fetched_partition.messages.first.is_a?(Kafka::Protocol::MessageSet)
          extract_messages
        else
          extract_records
        end

      FetchedBatch.new(
        topic: @topic,
        partition: @fetched_partition.partition,
        highwater_mark_offset: @fetched_partition.highwater_mark_offset,
        messages: messages
      )
    end

    private

    def extract_messages
      @fetched_partition.messages.flat_map do |message_set|
        message_set.messages.map do |message|
          FetchedMessage.new(
            message: message,
            topic: @topic,
            partition: @fetched_partition.partition
          )
        end
      end
    end

    def extract_records
      records = []
      aborted_transactions = @fetched_partition.aborted_transactions.sort_by(&:first_offset)
      aborted_producer_ids = {}

      @fetched_partition.messages.each do |record_batch|
        # Find the list of aborted producer IDs less than current offset
        unless aborted_transactions.empty?
          if aborted_transactions.first.first_offset <= record_batch.last_offset
            aborted_transaction = aborted_transactions.shift
            aborted_producer_ids[aborted_transaction.producer_id] = aborted_transaction.first_offset
          end
        end

        if abort_marker?(record_batch)
          # Abort marker, remove the producer from the aborted list
          aborted_producer_ids.delete(record_batch.producer_id)
        elsif aborted_producer_ids.key?(record_batch.producer_id) && record_batch.in_transaction
          # Reject aborted record batch
          @logger.info("Reject #{record_batch.records.size} aborted records of topic '#{@topic}', partition #{@fetched_partition.partition}, from offset #{record_batch.first_offset}")
          next
        end

        record_batch.records.each do |record|
          unless record.is_control_record
            records << FetchedMessage.new(
              message: record,
              topic: @topic,
              partition: @fetched_partition.partition
            )
          end
        end
      end
      records
    end

    def abort_marker?(record_batch)
      return false unless record_batch.is_control_batch

      if record_batch.records.empty?
        raise "Invalid control record batch at topic '#{@topic}', partition #{@fetched_partition}"
      end

      record_batch.records.first.key == ABORTED_TRANSACTION_SIGNAL
    end
  end
end
