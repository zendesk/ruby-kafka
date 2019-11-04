# frozen_string_literal: true

require "kafka/protocol/message_set"
require "kafka/protocol/record_batch"

module Kafka
  # A produce operation attempts to send all messages in a buffer to the Kafka cluster.
  # Since topics and partitions are spread among all brokers in a cluster, this usually
  # involves sending requests to several or all of the brokers.
  #
  # ## Instrumentation
  #
  # When executing the operation, an `ack_message.producer.kafka` notification will be
  # emitted for each message that was successfully appended to a topic partition.
  # The following keys will be found in the payload:
  #
  # * `:topic` — the topic that was written to.
  # * `:partition` — the partition that the message set was appended to.
  # * `:offset` — the offset of the message in the partition.
  # * `:key` — the message key.
  # * `:value` — the message value.
  # * `:delay` — the time between the message was produced and when it was acknowledged.
  #
  # In addition to these notifications, a `send_messages.producer.kafka` notification will
  # be emitted after the operation completes, regardless of whether it succeeds. This
  # notification will have the following keys:
  #
  # * `:message_count` – the total number of messages that the operation tried to
  #   send. Note that not all messages may get delivered.
  # * `:sent_message_count` – the number of messages that were successfully sent.
  #
  class ProduceOperation
    def initialize(cluster:, transaction_manager:, buffer:, compressor:, required_acks:, ack_timeout:, logger:, instrumenter:)
      @cluster = cluster
      @transaction_manager = transaction_manager
      @buffer = buffer
      @required_acks = required_acks
      @ack_timeout = ack_timeout
      @compressor = compressor
      @logger = TaggedLogger.new(logger)
      @instrumenter = instrumenter
    end

    def execute
      if (@transaction_manager.idempotent? || @transaction_manager.transactional?) && @required_acks != -1
        raise 'You must set required_acks option to :all to use idempotent / transactional production'
      end

      if @transaction_manager.transactional? && !@transaction_manager.in_transaction?
        raise "Produce operation can only be executed in a pending transaction"
      end

      @instrumenter.instrument("send_messages.producer") do |notification|
        message_count = @buffer.size

        notification[:message_count] = message_count

        begin
          if @transaction_manager.idempotent? || @transaction_manager.transactional?
            @transaction_manager.init_producer_id
          end
          send_buffered_messages
        ensure
          notification[:sent_message_count] = message_count - @buffer.size
        end
      end
    end

    private

    def send_buffered_messages
      messages_for_broker = {}
      topic_partitions = {}

      @buffer.each do |topic, partition, messages|
        begin
          broker = @cluster.get_leader(topic, partition)

          @logger.debug "Current leader for #{topic}/#{partition} is node #{broker}"

          topic_partitions[topic] ||= Set.new
          topic_partitions[topic].add(partition)

          messages_for_broker[broker] ||= MessageBuffer.new
          messages_for_broker[broker].concat(messages, topic: topic, partition: partition)
        rescue Kafka::Error => e
          @logger.error "Could not connect to leader for partition #{topic}/#{partition}: #{e.message}"

          @instrumenter.instrument("topic_error.producer", {
            topic: topic,
            exception: [e.class.to_s, e.message],
          })

          # We can't send the messages right now, so we'll just keep them in the buffer.
          # We'll mark the cluster as stale in order to force a metadata refresh.
          @cluster.mark_as_stale!
        end
      end

      # Add topic and partition to transaction
      if @transaction_manager.transactional?
        @transaction_manager.add_partitions_to_transaction(topic_partitions)
      end

      messages_for_broker.each do |broker, message_buffer|
        begin
          @logger.info "Sending #{message_buffer.size} messages to #{broker}"

          records_for_topics = {}

          message_buffer.each do |topic, partition, records|
            record_batch = Protocol::RecordBatch.new(
              records: records,
              first_sequence: @transaction_manager.next_sequence_for(
                topic, partition
              ),
              in_transaction: @transaction_manager.transactional?,
              producer_id: @transaction_manager.producer_id,
              producer_epoch: @transaction_manager.producer_epoch
            )
            records_for_topics[topic] ||= {}
            records_for_topics[topic][partition] = record_batch
          end

          response = broker.produce(
            messages_for_topics: records_for_topics,
            compressor: @compressor,
            required_acks: @required_acks,
            timeout: @ack_timeout * 1000, # Kafka expects the timeout in milliseconds.
            transactional_id: @transaction_manager.transactional_id
          )

          handle_response(broker, response, records_for_topics) if response
        rescue ConnectionError => e
          @logger.error "Could not connect to broker #{broker}: #{e}"

          # Mark the cluster as stale in order to force a cluster metadata refresh.
          @cluster.mark_as_stale!
        end
      end
    end

    def handle_response(broker, response, records_for_topics)
      response.each_partition do |topic_info, partition_info|
        topic = topic_info.topic
        partition = partition_info.partition
        record_batch = records_for_topics[topic][partition]
        records = record_batch.records
        ack_time = Time.now

        begin
          begin
            Protocol.handle_error(partition_info.error_code)
          rescue ProtocolError => e
            @instrumenter.instrument("topic_error.producer", {
              topic: topic,
              exception: [e.class.to_s, e.message],
            })

            raise e
          end

          if @transaction_manager.idempotent? || @transaction_manager.transactional?
            @transaction_manager.update_sequence_for(
              topic, partition, record_batch.first_sequence + record_batch.size
            )
          end

          records.each_with_index do |record, index|
            @instrumenter.instrument("ack_message.producer", {
              key: record.key,
              value: record.value,
              topic: topic,
              partition: partition,
              offset: partition_info.offset + index,
              delay: ack_time - record.create_time,
            })
          end
        rescue Kafka::CorruptMessage
          @logger.error "Corrupt message when writing to #{topic}/#{partition} on #{broker}"
        rescue Kafka::UnknownTopicOrPartition
          @logger.error "Unknown topic or partition #{topic}/#{partition} on #{broker}"
          @cluster.mark_as_stale!
        rescue Kafka::LeaderNotAvailable
          @logger.error "Leader currently not available for #{topic}/#{partition}"
          @cluster.mark_as_stale!
        rescue Kafka::NotLeaderForPartition
          @logger.error "Broker #{broker} not currently leader for #{topic}/#{partition}"
          @cluster.mark_as_stale!
        rescue Kafka::RequestTimedOut
          @logger.error "Timed out while writing to #{topic}/#{partition} on #{broker}"
        rescue Kafka::NotEnoughReplicas
          @logger.error "Not enough in-sync replicas for #{topic}/#{partition}"
        rescue Kafka::NotEnoughReplicasAfterAppend
          @logger.error "Messages written, but to fewer in-sync replicas than required for #{topic}/#{partition}"
        else
          @logger.debug "Successfully appended #{records.count} messages to #{topic}/#{partition} on #{broker}"

          # The messages were successfully written; clear them from the buffer.
          @buffer.clear_messages(topic: topic, partition: partition)
        end
      end
    end
  end
end
