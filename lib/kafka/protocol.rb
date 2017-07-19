module Kafka
  module Protocol
    # The replica id of non-brokers is always -1.
    REPLICA_ID = -1

    PRODUCE_API = 0
    FETCH_API = 1
    LIST_OFFSET_API = 2
    TOPIC_METADATA_API = 3
    OFFSET_COMMIT_API = 8
    OFFSET_FETCH_API = 9
    GROUP_COORDINATOR_API = 10
    JOIN_GROUP_API = 11
    HEARTBEAT_API = 12
    LEAVE_GROUP_API = 13
    SYNC_GROUP_API = 14
    SASL_HANDSHAKE_API = 17

    APIS = {
      PRODUCE_API => :produce,
      FETCH_API => :fetch,
      LIST_OFFSET_API => :list_offset,
      TOPIC_METADATA_API => :topic_metadata,
      OFFSET_COMMIT_API => :offset_commit,
      OFFSET_FETCH_API => :offset_fetch,
      GROUP_COORDINATOR_API => :group_coordinator,
      JOIN_GROUP_API => :join_group,
      HEARTBEAT_API => :heartbeat,
      LEAVE_GROUP_API => :leave_group,
      SYNC_GROUP_API => :sync_group,
      SASL_HANDSHAKE_API => :sasl_handshake,
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
      28 => InvalidCommitOffsetSize,
      29 => TopicAuthorizationCode,
      30 => GroupAuthorizationCode,
      31 => ClusterAuthorizationCode,
      32 => InvalidTimestamp,
      33 => UnsupportedSaslMechanism,
      34 => InvalidSaslState,
      35 => UnsupportedVersion,
      36 => TopicAlreadyExists,
      37 => InvalidPartitions,
      38 => InvalidReplicationFactor,
      39 => InvalidReplicaAssignment,
      40 => InvalidConfig,
      41 => NotController,
      42 => InvalidRequest
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
require "kafka/protocol/sasl_handshake_request"
require "kafka/protocol/sasl_handshake_response"
