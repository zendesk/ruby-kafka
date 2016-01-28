module Kafka
  module Protocol
    PRODUCE_API_KEY = 0
    TOPIC_METADATA_API_KEY = 3

    ERRORS = {
      -1 => UnknownError,
      1 => OffsetOutOfRange,
      2 => CorruptMessage,
      3 => UnknownTopicOrPartition,
      4 => InvalidMessageSize,
      5 => LeaderNotAvailable,
      6 => NotLeaderForPartition,
      7 => RequestTimedOut,
      8 => BrokerNotAvailable,
      9 => ReplicaNotAvailable,
      10 => MessageSizeTooLarge,
      12 => OffsetMetadataTooLarge,
      17 => InvalidTopic,
      18 => RecordListTooLarge,
      19 => NotEnoughReplicas,
      20 => NotEnoughReplicasAfterAppend,
      21 => InvalidRequiredAcks,
    }


    def self.handle_error(error_code)
      if error_code == 0
        # No errors, yay!
      elsif error = ERRORS[error_code]
        raise error
      else
        raise UnknownError, "Unknown error with code #{error_code}"
      end
    end
  end
end

require "kafka/protocol/topic_metadata_request"
require "kafka/protocol/metadata_response"
require "kafka/protocol/produce_request"
require "kafka/protocol/produce_response"
