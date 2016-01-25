module Kafka
  module Protocol
    PRODUCE_API_KEY = 0
    TOPIC_METADATA_API_KEY = 3

    def self.handle_error(error_code)
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
      when 9 then raise ReplicaNotAvailable
      else raise UnknownError, "Unknown error with code #{error_code}"
      end
    end
  end
end

require "kafka/protocol/topic_metadata_request"
require "kafka/protocol/metadata_response"
require "kafka/protocol/produce_request"
require "kafka/protocol/produce_response"
