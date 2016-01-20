require "kafka/version"
require "kafka/broker"
require "kafka/producer"

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

  def self.new(**options)
    Broker.connect(**options)
  end
end
