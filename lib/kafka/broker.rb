require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def initialize(connection:, node_id: nil, logger:)
      @connection = connection
      @node_id = node_id
      @logger = logger
    end

    def to_s
      "#{@connection} (node_id=#{@node_id.inspect})"
    end

    def disconnect
      @connection.close
    end

    def fetch_metadata(**options)
      request = Protocol::TopicMetadataRequest.new(**options)
      response_class = Protocol::MetadataResponse

      @connection.send_request(request, response_class)
    end

    def fetch_messages(**options)
      request = Protocol::FetchRequest.new(**options)
      response_class = Protocol::FetchResponse

      @connection.send_request(request, response_class)
    end

    def list_offsets(**options)
      request = Protocol::ListOffsetRequest.new(**options)
      response_class = Protocol::ListOffsetResponse

      @connection.send_request(request, response_class)
    end

    def produce(**options)
      request = Protocol::ProduceRequest.new(**options)
      response_class = request.requires_acks? ? Protocol::ProduceResponse : nil

      @connection.send_request(request, response_class)
    end
  end
end
