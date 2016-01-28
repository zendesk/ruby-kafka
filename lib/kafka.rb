require "kafka/version"

module Kafka
  Error = Class.new(StandardError)

  ProtocolError = Class.new(StandardError)
  CorruptMessage = Class.new(ProtocolError)
  UnknownError = Class.new(ProtocolError)
  OffsetOutOfRange = Class.new(ProtocolError)
  UnknownTopicOrPartition = Class.new(ProtocolError)
  InvalidMessageSize = Class.new(ProtocolError)
  LeaderNotAvailable = Class.new(ProtocolError)
  NotLeaderForPartition = Class.new(ProtocolError)
  RequestTimedOut = Class.new(ProtocolError)
  BrokerNotAvailable = Class.new(ProtocolError)
  MessageSizeTooLarge = Class.new(ProtocolError)
  OffsetMetadataTooLarge = Class.new(ProtocolError)
  InvalidTopic = Class.new(ProtocolError)
  RecordListTooLarge = Class.new(ProtocolError)
  NotEnoughReplicas = Class.new(ProtocolError)
  NotEnoughReplicasAfterAppend = Class.new(ProtocolError)
  InvalidRequiredAcks = Class.new(ProtocolError)

  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  ReplicaNotAvailable = Class.new(ProtocolError)

  # Raised when there's a network connection error.
  ConnectionError = Class.new(Error)

  # Raised when a producer buffer has reached its maximum size.
  BufferOverflow = Class.new(Error)

  # Raised if not all messages could be sent by a producer.
  FailedToSendMessages = Class.new(Error)

  # Initializes a new Kafka client.
  #
  # @see Client#initialize
  # @return [Client]
  def self.new(**options)
    Client.new(**options)
  end
end

require "kafka/client"
