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
      request = Kafka::Protocol::TopicMetadataRequest.new(**options)
      response = Kafka::Protocol::MetadataResponse.new

      @connection.write_request(request)
      @connection.read_response(response)

      response
    end
  end
end
