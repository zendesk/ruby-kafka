require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def self.connect(host:, port:, client_id:, logger:)
      connection = Connection.open(
        host: host,
        port: port.to_i,
        client_id: client_id,
        logger: logger
      )

      new(connection: connection, logger: logger)
    end

    def initialize(connection:, logger: nil)
      @connection = connection
      @logger = logger
    end

    def fetch_metadata(**options)
      api_key = Protocol::TOPIC_METADATA_API_KEY
      request = Protocol::TopicMetadataRequest.new(**options)
      response = Protocol::MetadataResponse.new

      @connection.write_request(api_key, request)
      @connection.read_response(response)

      response
    end

    def produce(**options)
      api_key = Protocol::PRODUCE_API_KEY
      request = Protocol::ProduceRequest.new(**options)

      @connection.write_request(api_key, request)

      if request.requires_acks?
        response = Protocol::ProduceResponse.new
        @connection.read_response(response)
        response
      else
        nil
      end
    end
  end
end
