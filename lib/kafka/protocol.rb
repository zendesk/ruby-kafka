module Kafka
  module Protocol
    # The replica id of non-brokers is always -1.
    REPLICA_ID = -1

    APIS = {
      0 => :produce,
      1 => :fetch,
      2 => :list_offset,
      3 => :topic_metadata,
      8 => :offset_commit,
      9 => :offset_fetch,
      10 => :group_coordinator,
      11 => :join_group,
      12 => :heartbeat,
      13 => :leave_group,
      14 => :sync_group,
    }

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
      15 => GroupCoordinatorNotAvailable,
      16 => NotCoordinatorForGroup,
      17 => InvalidTopic,
      18 => RecordListTooLarge,
      19 => NotEnoughReplicas,
      20 => NotEnoughReplicasAfterAppend,
      21 => InvalidRequiredAcks,
      22 => IllegalGeneration,
      25 => UnknownMemberId,
      26 => InvalidSessionTimeout,
      27 => RebalanceInProgress,
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

    def self.api_name(api_key)
      APIS.fetch(api_key, :unknown)
    end
  end
end

require "kafka/protocol/topic_metadata_request"
require "kafka/protocol/metadata_response"
require "kafka/protocol/produce_request"
require "kafka/protocol/produce_response"
require "kafka/protocol/fetch_request"
require "kafka/protocol/fetch_response"
require "kafka/protocol/list_offset_request"
require "kafka/protocol/list_offset_response"
require "kafka/protocol/group_coordinator_request"
require "kafka/protocol/group_coordinator_response"
require "kafka/protocol/join_group_request"
require "kafka/protocol/join_group_response"
require "kafka/protocol/sync_group_request"
require "kafka/protocol/sync_group_response"
require "kafka/protocol/leave_group_request"
require "kafka/protocol/leave_group_response"
require "kafka/protocol/heartbeat_request"
require "kafka/protocol/heartbeat_response"
require "kafka/protocol/offset_fetch_request"
require "kafka/protocol/offset_fetch_response"
require "kafka/protocol/offset_commit_request"
require "kafka/protocol/offset_commit_response"
require "kafka/protocol/async_response"
