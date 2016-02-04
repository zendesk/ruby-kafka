module Kafka
  class Transmission
    def initialize(broker_pool:, buffer:, required_acks:, ack_timeout:, logger:)
      @broker_pool = broker_pool
      @buffer = buffer
      @required_acks = required_acks
      @ack_timeout = ack_timeout
      @logger = logger
    end

    def send_messages
      messages_for_broker = {}

      @buffer.each do |topic, partition, messages|
        broker = @broker_pool.get_leader(topic, partition)

        @logger.debug "Current leader for #{topic}/#{partition} is node #{broker}"

        messages_for_broker[broker] ||= MessageBuffer.new
        messages_for_broker[broker].concat(messages, topic: topic, partition: partition)
      end

      messages_for_broker.each do |broker, message_set|
        begin
          response = broker.produce(
            messages_for_topics: message_set.to_h,
            required_acks: @required_acks,
            timeout: @ack_timeout * 1000, # Kafka expects the timeout in milliseconds.
          )

          handle_response(response) if response
        rescue ConnectionError => e
          @logger.error "Could not connect to broker #{broker}: #{e}"

          # Mark the broker pool as stale in order to force a cluster metadata refresh.
          @broker_pool.mark_as_stale!
        end
      end
    end

    private

    def handle_response(response)
      response.each_partition do |topic_info, partition_info|
        topic = topic_info.topic
        partition = partition_info.partition

        begin
          Protocol.handle_error(partition_info.error_code)
        rescue Kafka::CorruptMessage
          @logger.error "Corrupt message when writing to #{topic}/#{partition}"
        rescue Kafka::UnknownTopicOrPartition
          @logger.error "Unknown topic or partition #{topic}/#{partition}"
        rescue Kafka::LeaderNotAvailable
          @logger.error "Leader currently not available for #{topic}/#{partition}"
          @broker_pool.mark_as_stale!
        rescue Kafka::NotLeaderForPartition
          @logger.error "Broker not currently leader for #{topic}/#{partition}"
          @broker_pool.mark_as_stale!
        rescue Kafka::RequestTimedOut
          @logger.error "Timed out while writing to #{topic}/#{partition}"
        rescue Kafka::NotEnoughReplicas
          @logger.error "Not enough in-sync replicas for #{topic}/#{partition}"
        rescue Kafka::NotEnoughReplicasAfterAppend
          @logger.error "Messages written, but to fewer in-sync replicas than required for #{topic}/#{partition}"
        else
          offset = partition_info.offset
          @logger.info "Successfully sent messages for #{topic}/#{partition}; new offset is #{offset}"

          # The messages were successfully written; clear them from the buffer.
          @buffer.clear_messages(topic: topic, partition: partition)
        end
      end
    end
  end
end
