# frozen_string_literal: true

require "kafka/version"

module Kafka
  class Error < StandardError
  end

  # There was an error processing a message.
  class ProcessingError < Error
    attr_reader :topic, :partition, :offset

    def initialize(topic, partition, offset)
      @topic = topic
      @partition = partition
      @offset = offset

      super()
    end
  end

  # Subclasses of this exception class map to an error code described in the
  # Kafka protocol specification.
  # https://kafka.apache.org/protocol#protocol_error_codes
  class ProtocolError < Error
  end

  # -1
  # The server experienced an unexpected error when processing the request
  class UnknownError < ProtocolError
  end

  # 1
  # The requested offset is not within the range of offsets maintained by the server.
  class OffsetOutOfRange < ProtocolError
    attr_accessor :topic, :partition, :offset
  end

  # 2
  # This indicates that a message contents does not match its CRC.
  class CorruptMessage < ProtocolError
  end

  # 3
  # The request is for a topic or partition that does not exist on the broker.
  class UnknownTopicOrPartition < ProtocolError
  end

  # 4
  # The message has a negative size.
  class InvalidMessageSize < ProtocolError
  end

  # 5
  # This error is thrown if we are in the middle of a leadership election and
  # there is currently no leader for this partition and hence it is unavailable
  # for writes.
  class LeaderNotAvailable < ProtocolError
  end

  # 6
  # This error is thrown if the client attempts to send messages to a replica
  # that is not the leader for some partition. It indicates that the client's
  # metadata is out of date.
  class NotLeaderForPartition < ProtocolError
  end

  # 7
  # This error is thrown if the request exceeds the user-specified time limit
  # in the request.
  class RequestTimedOut < ProtocolError
  end

  # 8
  # The broker is not available.
  class BrokerNotAvailable < ProtocolError
  end

  # 9
  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  class ReplicaNotAvailable < ProtocolError
  end

  # 10
  # The server has a configurable maximum message size to avoid unbounded memory
  # allocation. This error is thrown if the client attempt to produce a message
  # larger than this maximum.
  class MessageSizeTooLarge < ProtocolError
  end

  # 11
  # The controller moved to another broker.
  class StaleControllerEpoch < ProtocolError
  end

  # 12
  # If you specify a string larger than configured maximum for offset metadata.
  class OffsetMetadataTooLarge < ProtocolError
  end

  # 13
  # The server disconnected before a response was received.
  class NetworkException < ProtocolError
  end

  # 14
  # The coordinator is loading and hence can't process requests.
  class CoordinatorLoadInProgress < ProtocolError
  end

  # 15
  # The coordinator is not available.
  class CoordinatorNotAvailable < ProtocolError
  end

  # 16
  # This is not the correct coordinator.
  class NotCoordinatorForGroup < ProtocolError
  end

  # 17
  # For a request which attempts to access an invalid topic (e.g. one which has
  # an illegal name), or if an attempt is made to write to an internal topic
  # (such as the consumer offsets topic).
  class InvalidTopic < ProtocolError
  end

  # 18
  # If a message batch in a produce request exceeds the maximum configured
  # segment size.
  class RecordListTooLarge < ProtocolError
  end

  # 19
  # Returned from a produce request when the number of in-sync replicas is
  # lower than the configured minimum and requiredAcks is -1.
  class NotEnoughReplicas < ProtocolError
  end

  # 20
  # Returned from a produce request when the message was written to the log,
  # but with fewer in-sync replicas than required.
  class NotEnoughReplicasAfterAppend < ProtocolError
  end

  # 21
  # Returned from a produce request if the requested requiredAcks is invalid
  # (anything other than -1, 1, or 0).
  class InvalidRequiredAcks < ProtocolError
  end

  # 22
  # Specified group generation id is not valid.
  class IllegalGeneration < ProtocolError
  end

  # 23
  # The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.
  class InconsistentGroupProtocol < ProtocolError
  end

  # 24
  # The configured groupId is invalid
  class InvalidGroupId < ProtocolError
  end

  # 25
  # The coordinator is not aware of this member.
  class UnknownMemberId < ProtocolError
  end

  # 26
  # The session timeout is not within the range allowed by the broker
  class InvalidSessionTimeout < ProtocolError
  end

  # 27
  # The group is rebalancing, so a rejoin is needed.
  class RebalanceInProgress < ProtocolError
  end

  # 28
  # The committing offset data size is not valid
  class InvalidCommitOffsetSize < ProtocolError
  end

  # 29
  class TopicAuthorizationFailed < ProtocolError
  end

  # 30
  class GroupAuthorizationFailed < ProtocolError
  end

  # 31
  class ClusterAuthorizationFailed < ProtocolError
  end

  # 32
  # The timestamp of the message is out of acceptable range.
  class InvalidTimestamp < ProtocolError
  end

  # 33
  # The broker does not support the requested SASL mechanism.
  class UnsupportedSaslMechanism < ProtocolError
  end

  # 34
  class InvalidSaslState < ProtocolError
  end

  # 35
  class UnsupportedVersion < ProtocolError
  end

  # 36
  class TopicAlreadyExists < ProtocolError
  end

  # 37
  # Number of partitions is below 1.
  class InvalidPartitions < ProtocolError
  end

  # 38
  # Replication factor is below 1 or larger than the number of available brokers.
  class InvalidReplicationFactor < ProtocolError
  end

  # 39
  class InvalidReplicaAssignment < ProtocolError
  end

  # 40
  class InvalidConfig < ProtocolError
  end

  # 41
  # This is not the correct controller for this cluster.
  class NotController < ProtocolError
  end

  # 42
  class InvalidRequest < ProtocolError
  end

  # 43
  # The message format version on the broker does not support the request.
  class UnsupportedForMessageFormat < ProtocolError
  end

  # 44
  # Request parameters do not satisfy the configured policy.
  class PolicyViolation < ProtocolError
  end

  # 45
  # The broker received an out of order sequence number
  class OutOfOrderSequenceNumberError < Error
  end

  # 46
  # The broker received a duplicate sequence number
  class DuplicateSequenceNumberError < Error
  end

  # 47
  # Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
  class InvalidProducerEpochError < Error
  end

  # 48
  # The producer attempted a transactional operation in an invalid state
  class InvalidTxnStateError < Error
  end

  # 49
  # The producer attempted to use a producer id which is not currently assigned to its transactional id
  class InvalidProducerIDMappingError < Error
  end

  # 50
  # The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
  class InvalidTransactionTimeoutError < Error
  end

  # 51
  # The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
  class ConcurrentTransactionError < Error
  end

  # 52
  # Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
  class TransactionCoordinatorFencedError < Error
  end

  ###
  # ruby-kafka errors
  ###

  # A fetch operation was executed with no partitions specified.
  class NoPartitionsToFetchFrom < Error
  end

  # A message in a partition is larger than the maximum we've asked for.
  class MessageTooLargeToRead < Error
  end

  # A connection has been unused for too long, we assume the server has killed it.
  class IdleConnection < Error
  end

  # When the record array length doesn't match real number of received records
  class InsufficientDataMessage < Error
  end
  # Raised when there's a network connection error.
  class ConnectionError < Error
  end

  class NoSuchBroker < Error
  end

  # Raised when a producer buffer has reached its maximum size.
  class BufferOverflow < Error
  end

  # Raised if not all messages could be sent by a producer.
  class DeliveryFailed < Error
    attr_reader :failed_messages

    def initialize(message, failed_messages)
      @failed_messages = failed_messages

      super(message)
    end
  end

  class HeartbeatError < Error
  end

  class OffsetCommitError < Error
  end

  class FetchError < Error
  end

  class SaslScramError < Error
  end

  class FailedScramAuthentication < SaslScramError
  end

  # The Token Provider object used for SASL OAuthBearer does not implement the method `token`
  class TokenMethodNotImplementedError < Error
  end

  # Initializes a new Kafka client.
  #
  # @see Client#initialize
  # @return [Client]
  def self.new(seed_brokers = nil, **options)
    # We allow `seed_brokers` to be passed in either as a positional _or_ as a
    # keyword argument.
    if seed_brokers.nil?
      Client.new(**options)
    else
      Client.new(seed_brokers: seed_brokers, **options)
    end
  end
end

require "kafka/client"
