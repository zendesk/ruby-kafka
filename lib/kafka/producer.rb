require "kafka/message"
require "kafka/message_set"

module Kafka
  class Producer
    # @param timeout [Integer] The number of milliseconds to wait for an
    #   acknowledgement from the broker before timing out.
    # @param required_acks [Integer] The number of replicas that must acknowledge
    #   a write.
    def initialize(broker_pool:, logger:, timeout: 10_000, required_acks: 1)
      @broker_pool = broker_pool
      @logger = logger
      @required_acks = required_acks
      @timeout = timeout
      @buffered_messages = []
    end

    def write(value, key:, topic:, partition:)
      @buffered_messages << Message.new(value, key: key, topic: topic, partition: partition)
    end

    def flush
      messages_for_broker = {}

      @buffered_messages.each do |message|
        broker = @broker_pool.get_leader(message.topic, message.partition)

        messages_for_broker[broker] ||= []
        messages_for_broker[broker] << message
      end

      messages_for_broker.each do |broker, messages|
        @logger.info "Sending #{messages.count} messages to broker #{broker}"

        message_set = MessageSet.new(messages)

        response = broker.produce(
          messages_for_topics: message_set.to_h,
          required_acks: @required_acks,
          timeout: @timeout,
        )

        if response
          response.topics.each do |topic_info|
            topic_info.partitions.each do |partition_info|
              handle_error_code(partition_info.error_code)
            end
          end
        end
      end

      @buffered_messages.clear
    end

    private

    def handle_error_code(error_code)
      case error_code
      when -1 then raise UnknownError
      when 0 then nil # no error, yay!
      when 1 then raise OffsetOutOfRange
      when 2 then raise CorruptMessage
      when 3 then raise UnknownTopicOrPartition
      when 4 then raise InvalidMessageSize
      when 5 then raise LeaderNotAvailable
      when 6 then raise NotLeaderForPartition
      when 7 then raise RequestTimedOut
      else raise UnknownError, "Unknown error with code #{error_code}"
      end
    end
  end
end
