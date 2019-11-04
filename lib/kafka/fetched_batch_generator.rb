# frozen_string_literal: true

require "kafka/fetched_batch"

module Kafka
  class FetchedBatchGenerator
    COMMITTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x01".freeze
    ABORTED_TRANSACTION_SIGNAL = "\x00\x00\x00\x00".freeze

    def initialize(topic, fetched_partition, offset, logger:)
      @topic = topic
      @fetched_partition = fetched_partition
      @logger = TaggedLogger.new(logger)
      @offset = offset
    end

    def generate
      if @fetched_partition.messages.empty?
        empty_fetched_batch
      elsif @fetched_partition.messages.first.is_a?(Kafka::Protocol::MessageSet)
        extract_messages
      else
        extract_records
      end
    end

    private

    def empty_fetched_batch
      FetchedBatch.new(
        topic: @topic,
        partition: @fetched_partition.partition,
        last_offset: nil,
        highwater_mark_offset: @fetched_partition.highwater_mark_offset,
        messages: []
      )
    end

    def extract_messages
      last_offset = nil
      messages = @fetched_partition.messages.flat_map do |message_set|
        message_set.messages.map do |message|
          last_offset = message.offset if last_offset.nil? || last_offset < message.offset
          if message.offset >= @offset
            FetchedMessage.new(
              message: message,
              topic: @topic,
              partition: @fetched_partition.partition
            )
          end
        end.compact
      end
      FetchedBatch.new(
        topic: @topic,
        partition: @fetched_partition.partition,
        last_offset: last_offset,
        highwater_mark_offset: @fetched_partition.highwater_mark_offset,
        messages: messages
      )
    end

    def extract_records
      records = []
      last_offset = nil
      leader_epoch = nil
      aborted_transactions = @fetched_partition.aborted_transactions.sort_by(&:first_offset)
      aborted_producer_ids = {}

      @fetched_partition.messages.each do |record_batch|
        last_offset = record_batch.last_offset if last_offset.nil? || last_offset < record_batch.last_offset
        leader_epoch = record_batch.partition_leader_epoch if leader_epoch.nil? || leader_epoch < record_batch.partition_leader_epoch
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
          if !record.is_control_record && record.offset >= @offset
            records << FetchedMessage.new(
              message: record,
              topic: @topic,
              partition: @fetched_partition.partition
            )
          end
        end
      end

      FetchedBatch.new(
        topic: @topic,
        partition: @fetched_partition.partition,
        last_offset: last_offset,
        leader_epoch: leader_epoch,
        highwater_mark_offset: @fetched_partition.highwater_mark_offset,
        messages: records
      )
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
