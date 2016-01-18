require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Cluster
    def self.connect(brokers:, client_id:, logger:)
      host, port = brokers.first.split(":", 2)

      connection = Connection.new(
        host: host,
        port: port.to_i,
        client_id: client_id,
        logger: logger
      )

      connection.open

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
  end
end
