require "kafka/version"
require "kafka/client"

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

  # Raised when a producer buffer has reached its maximum size.
  BufferOverflow = Class.new(Error)

  # Raised if not all messages could be sent by a producer.
  FailedToSendMessages = Class.new(Error)

  # Raised if a replica is expected on a broker, but is not. Can be safely ignored.
  ReplicaNotAvailable = Class.new(Error)

  def self.new(**options)
    Client.new(**options)
  end
end
