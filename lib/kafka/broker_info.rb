# Represents a broker in a Kafka cluster.
module Kafka
  class BrokerInfo
    attr_reader :node_id, :host, :port

    def initialize(node_id:, host:, port:)
      @node_id = node_id
      @host = host
      @port = port
    end

    def to_s
      "#{host}:#{port} (node_id=#{node_id})"
    end
  end
end
