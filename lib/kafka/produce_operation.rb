require "kafka/protocol/produce_request"

module Kafka
  # A produce operation attempts to send all messages in a buffer to the Kafka cluster.
  # Since topics and partitions are spread among all brokers in a cluster, this usually
  # involves sending requests to several or all of the brokers.
  #
  # ## Instrumentation
  #
  # When executing the operation, an `append_message_set.kafka` notification will be
  # emitted for each message set that was successfully appended to a topic partition.
  # The following keys will be found in the payload:
  #
  # * `:topic` — the topic that was written to.
  # * `:partition` — the partition that the message set was appended to.
  # * `:offset` — the offset of the first message in the message set.
  # * `:message_count` — the number of messages that were appended.
  #
  # If there was an error appending the message set, the key `:exception` will be set
  # in the payload. In that case, the message set will most likely not have been
  # appended and will possibly be retried later. Check this key before reporting the
  # operation as successful.
  #
  class ProduceOperation
    def initialize(cluster:, buffer:, required_acks:, ack_timeout:, logger:)
      @cluster = cluster
      @buffer = buffer
      @required_acks = required_acks
      @ack_timeout = ack_timeout
      @logger = logger
    end

    def execute
      messages_for_broker = {}

      @buffer.each do |topic, partition, messages|
        begin
          connection = @cluster.get_leader(topic, partition)

          @logger.debug "Current leader for #{topic}/#{partition} is #{connection}"

          messages_for_broker[connection] ||= MessageBuffer.new
          messages_for_broker[connection].concat(messages, topic: topic, partition: partition)
        rescue Kafka::Error => e
          @logger.error "Could not connect to leader for partition #{topic}/#{partition}: #{e}"

          # We can't send the messages right now, so we'll just keep them in the buffer.
          # We'll mark the cluster as stale in order to force a metadata refresh.
          @cluster.mark_as_stale!
        end
      end

      messages_for_broker.each do |connection, message_set|
        begin
          @logger.info "Sending #{message_set.size} messages to #{connection}"

          request = Protocol::ProduceRequest.new(
            messages_for_topics: message_set.to_h,
            required_acks: @required_acks,
            timeout: @ack_timeout * 1000, # Kafka expects the timeout in milliseconds.
          )

          response = connection.send_request(request)

          handle_response(response) if response
        rescue ConnectionError => e
          @logger.error "Could not connect to #{connection}: #{e}"

          # Mark the cluster as stale in order to force a cluster metadata refresh.
          @cluster.mark_as_stale!
        end
      end
    end

    private

    def handle_response(response)
      response.each_partition do |topic_info, partition_info|
        topic = topic_info.topic
        partition = partition_info.partition
        offset = partition_info.offset
        message_count = @buffer.message_count_for_partition(topic: topic, partition: partition)

        begin
          payload = {
            topic: topic,
            partition: partition,
            offset: offset,
            message_count: message_count,
          }

          Instrumentation.instrument("append_message_set.kafka", payload) do
            Protocol.handle_error(partition_info.error_code)
          end
        rescue Kafka::CorruptMessage
          @logger.error "Corrupt message when writing to #{topic}/#{partition}"
        rescue Kafka::UnknownTopicOrPartition
          @logger.error "Unknown topic or partition #{topic}/#{partition}"
        rescue Kafka::LeaderNotAvailable
          @logger.error "Leader currently not available for #{topic}/#{partition}"
          @cluster.mark_as_stale!
        rescue Kafka::NotLeaderForPartition
          @logger.error "Broker not currently leader for #{topic}/#{partition}"
          @cluster.mark_as_stale!
        rescue Kafka::RequestTimedOut
          @logger.error "Timed out while writing to #{topic}/#{partition}"
        rescue Kafka::NotEnoughReplicas
          @logger.error "Not enough in-sync replicas for #{topic}/#{partition}"
        rescue Kafka::NotEnoughReplicasAfterAppend
          @logger.error "Messages written, but to fewer in-sync replicas than required for #{topic}/#{partition}"
        else
          @logger.debug "Successfully appended #{message_count} messages to #{topic}/#{partition} at offset #{offset}"

          # The messages were successfully written; clear them from the buffer.
          @buffer.clear_messages(topic: topic, partition: partition)
        end
      end
    end
  end
end
