# frozen_string_literal: true

require "kafka/version"

module Kafka
  # Set of default backoff values
  DEFAULT_BACKOFFS = {
    connection_error: 1,
    coordinator_not_available: 1,
    leader_not_available: 1,
    loop_max_reached: 1,
    loop_not_running: 0.1,
    message_delivery_error: 1,
    no_batches_to_process: 2,
    no_partitions_to_fetch_from: 1,
    not_coordinator_for_group: 1,
    offset_commit_error: 0.1,
    unexpected_error: 10,
    unknown_member_id: 1,
    unknown_topic_or_partition: 1
  }.freeze

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

  # A fetch operation was executed with no partitions specified.
  class NoPartitionsToFetchFrom < Error
  end

  # A message in a partition is larger than the maximum we've asked for.
  class MessageTooLargeToRead < Error
  end

  # A connection has been unused for too long, we assume the server has killed it.
  class IdleConnection < Error
  end

  # Subclasses of this exception class map to an error code described in the
  # Kafka protocol specification.
  #
  # See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
  class ProtocolError < Error
  end

  # This indicates that a message contents does not match its CRC.
  class CorruptMessage < ProtocolError
  end

  # When the record array length doesn't match real number of received records
  class InsufficientDataMessage < Error
  end

  class UnknownError < ProtocolError
  end

  class OffsetOutOfRange < ProtocolError
    attr_accessor :topic, :partition, :offset
  end

  # The request is for a topic or partition that does not exist on the broker.
  class UnknownTopicOrPartition < ProtocolError
  end

  # The message has a negative size.
  class InvalidMessageSize < ProtocolError
  end

  # This error is thrown if we are in the middle of a leadership election and
  # there is currently no leader for this partition and hence it is unavailable
  # for writes.
  class LeaderNotAvailable < ProtocolError
  end

  # This error is thrown if the client attempts to send messages to a replica
  # that is not the leader for some partition. It indicates that the client's
  # metadata is out of date.
  class NotLeaderForPartition < ProtocolError
  end

  # This error is thrown if the request exceeds the user-specified time limit
  # in the request.
  class RequestTimedOut < ProtocolError
  end

  class BrokerNotAvailable < ProtocolError
  end

  # The server has a configurable maximum message size to avoid unbounded memory
  # allocation. This error is thrown if the client attempt to produce a message
  # larger than this maximum.
  class MessageSizeTooLarge < ProtocolError
  end

  # If you specify a string larger than configured maximum for offset metadata.
  class OffsetMetadataTooLarge < ProtocolError
  end

  class CoordinatorNotAvailable < ProtocolError
  end

  class NotCoordinatorForGroup < ProtocolError
  end

  # 17
  # For a request which attempts to access an invalid topic (e.g. one which has
  # an illegal name), or if an attempt is made to write to an internal topic
  # (such as the consumer offsets topic).
  class InvalidTopic < ProtocolError
  end

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

  # 9
  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  class ReplicaNotAvailable < ProtocolError
  end

  # 25
  class UnknownMemberId < ProtocolError
  end

  # 27
  class RebalanceInProgress < ProtocolError
  end

  # 22
  class IllegalGeneration < ProtocolError
  end

  # 26
  class InvalidSessionTimeout < ProtocolError
  end

  # 28
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
  class InvalidTimestamp < ProtocolError
  end

  # 33
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
  class InvalidPartitions < ProtocolError
  end

  # 38
  class InvalidReplicationFactor < ProtocolError
  end

  # 39
  class InvalidReplicaAssignment < ProtocolError
  end

  # 40
  class InvalidConfig < ProtocolError
  end

  # 41
  class NotController < ProtocolError
  end

  # 42
  class InvalidRequest < ProtocolError
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

  # The broker received an out of order sequence number
  class OutOfOrderSequenceNumberError < Error
  end

  # The broker received a duplicate sequence number
  class DuplicateSequenceNumberError < Error
  end

  # Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
  class InvalidProducerEpochError < Error
  end

  # The producer attempted a transactional operation in an invalid state
  class InvalidTxnStateError < Error
  end

  # The producer attempted to use a producer id which is not currently assigned to its transactional id
  class InvalidProducerIDMappingError < Error
  end

  # The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).
  class InvalidTransactionTimeoutError < Error
  end

  # The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
  class ConcurrentTransactionError < Error
  end

  # Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
  class TransactionCoordinatorFencedError < Error
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
