require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def self.connect(node_id: nil, logger:, **options)
      connection = Connection.new(logger: logger, **options)
      new(connection: connection, node_id: node_id, logger: logger)
    end

    def initialize(connection:, node_id: nil, logger:)
      @connection = connection
      @node_id = node_id
      @logger = logger
    end

    # @return [String]
    def to_s
      "#{@connection} (node_id=#{@node_id.inspect})"
    end

    # @return [nil]
    def disconnect
      @connection.close
    end

    # Fetches cluster metadata from the broker.
    #
    # @param (see Kafka::Protocol::TopicMetadataRequest#initialize)
    # @return [Kafka::Protocol::MetadataResponse]
    def fetch_metadata(**options)
      request = Protocol::TopicMetadataRequest.new(**options)
      response_class = Protocol::MetadataResponse

      @connection.send_request(request, response_class)
    end

    # Fetches messages from a specified topic and partition.
    #
    # @param (see Kafka::Protocol::FetchRequest#initialize)
    # @return [Kafka::Protocol::FetchResponse]
    def fetch_messages(**options)
      request = Protocol::FetchRequest.new(**options)
      response_class = Protocol::FetchResponse

      @connection.send_request(request, response_class)
    end

    # Lists the offset of the specified topics and partitions.
    #
    # @param (see Kafka::Protocol::ListOffsetRequest#initialize)
    # @return [Kafka::Protocol::ListOffsetResponse]
    def list_offsets(**options)
      request = Protocol::ListOffsetRequest.new(**options)
      response_class = Protocol::ListOffsetResponse

      @connection.send_request(request, response_class)
    end

    # Produces a set of messages to the broker.
    #
    # @param (see Kafka::Protocol::ProduceRequest#initialize)
    # @return [Kafka::Protocol::ProduceResponse]
    def produce(**options)
      request = Protocol::ProduceRequest.new(**options)
      response_class = request.requires_acks? ? Protocol::ProduceResponse : nil

      @connection.send_request(request, response_class)
    end
  end
end
