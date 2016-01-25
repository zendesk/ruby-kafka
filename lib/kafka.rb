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
  ReplicaNotAvailable = Class.new(Error)

  def self.new(**options)
    Client.new(**options)
  end
end
