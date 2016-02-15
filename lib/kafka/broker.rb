require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def self.connect(logger:, **options)
      connection = Connection.new(logger: logger, **options)
      new(connection: connection, logger: logger)
    end

    def initialize(connection:, logger:)
      @connection = connection
      @logger = logger
    end

    def to_s
      @connection.to_s
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
