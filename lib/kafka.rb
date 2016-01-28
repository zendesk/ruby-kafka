require "kafka/version"

module Kafka
  Error = Class.new(StandardError)
  ConnectionError = Class.new(Error)
  CorruptMessage = Class.new(Error)
  UnknownError = Class.new(Error)
  OffsetOutOfRange = Class.new(Error)
  UnknownTopicOrPartition = Class.new(Error)
  InvalidMessageSize = Class.new(Error)
  LeaderNotAvailable = Class.new(Error)
  NotLeaderForPartition = Class.new(Error)
  RequestTimedOut = Class.new(Error)
  BrokerNotAvailable = Class.new(Error)
  MessageSizeTooLarge = Class.new(Error)
  OffsetMetadataTooLarge = Class.new(Error)
  InvalidTopic = Class.new(Error)
  RecordListTooLarge = Class.new(Error)
  NotEnoughReplicas = Class.new(Error)
  NotEnoughReplicasAfterAppend = Class.new(Error)
  InvalidRequiredAcks = Class.new(Error)

  # Raised when a producer buffer has reached its maximum size.
  BufferOverflow = Class.new(Error)

  # Raised if not all messages could be sent by a producer.
  FailedToSendMessages = Class.new(Error)

  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  ReplicaNotAvailable = Class.new(Error)

  # Initializes a new Kafka client.
  #
  # @see Client#initialize
  # @return [Client]
  def self.new(**options)
    Client.new(**options)
  end
end

require "kafka/client"
