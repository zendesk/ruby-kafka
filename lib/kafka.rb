require "kafka/version"
require "kafka/cluster"

module Kafka
  def self.new(**options)
    Cluster.new(**options)
  end
end
