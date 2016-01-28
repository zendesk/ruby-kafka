require "kafka/version"

module Kafka
  class Error < StandardError
  end

  # Subclasses of this exception class map to an error code described in the
  # Kafka protocol specification.
  #
  # See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
  class ProtocolError < StandardError
  end

  # This indicates that a message contents does not match its CRC.
  class CorruptMessage < ProtocolError
  end

  class UnknownError < ProtocolError
  end

  class OffsetOutOfRange < ProtocolError
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

  # For a request which attempts to access an invalid topic (e.g. one which has
  # an illegal name), or if an attempt is made to write to an internal topic
  # (such as the consumer offsets topic).
  class InvalidTopic < ProtocolError
  end

  # If a message batch in a produce request exceeds the maximum configured
  # segment size.
  class RecordListTooLarge < ProtocolError
  end

  # Returned from a produce request when the number of in-sync replicas is
  # lower than the configured minimum and requiredAcks is -1.
  class NotEnoughReplicas < ProtocolError
  end

  # Returned from a produce request when the message was written to the log,
  # but with fewer in-sync replicas than required.
  class NotEnoughReplicasAfterAppend < ProtocolError
  end

  # Returned from a produce request if the requested requiredAcks is invalid
  # (anything other than -1, 1, or 0).
  class InvalidRequiredAcks < ProtocolError
  end

  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  class ReplicaNotAvailable < ProtocolError
  end

  # Raised when there's a network connection error.
  class ConnectionError < Error
  end

  # Raised when a producer buffer has reached its maximum size.
  class BufferOverflow < Error
  end

  # Raised if not all messages could be sent by a producer.
  class FailedToSendMessages < Error
  end

  # Initializes a new Kafka client.
  #
  # @see Client#initialize
  # @return [Client]
  def self.new(**options)
    Client.new(**options)
  end
end

require "kafka/client"
