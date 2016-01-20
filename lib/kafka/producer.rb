require "kafka/protocol/message"

module Kafka
  class Producer
    # @param timeout [Integer] The number of milliseconds to wait for an
    #   acknowledgement from the broker before timing out.
    # @param required_acks [Integer] The number of replicas that must acknowledge
    #   a write.
    def initialize(broker:, logger:, timeout: 10_000, required_acks: 1)
      @broker = broker
      @logger = logger
      @required_acks = required_acks
      @timeout = timeout
      @buffer = {}
    end

    def write(value, key:, topic:, partition:)
      message = Protocol::Message.new(value: value, key: key)

      @buffer[topic] ||= {}
      @buffer[topic][partition] ||= []
      @buffer[topic][partition] << message
    end

    def flush
      response = @broker.produce(
        required_acks: @required_acks,
        timeout: @timeout,
        messages_for_topics: @buffer
      )

      if response
        response.topics.each do |topic_info|
          topic_info.partitions.each do |partition_info|
            handle_error_code(partition_info.error_code)
          end
        end
      end

      @buffer = {}
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
