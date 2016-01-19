require "kafka/version"
require "kafka/cluster"
require "kafka/producer"

module Kafka
  def self.new(**options)
    Cluster.new(**options)
  end
end
