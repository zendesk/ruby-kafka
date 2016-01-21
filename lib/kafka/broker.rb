require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def initialize(host:, port:, client_id:, logger:)
      @host, @port = host, port

      @connection = Connection.open(
        host: host,
        port: port,
        client_id: client_id,
        logger: logger
      )

      @logger = logger
    end

    def to_s
      "#{@host}:#{@port}"
    end

    def fetch_metadata(**options)
      api_key = Protocol::TOPIC_METADATA_API_KEY
      request = Protocol::TopicMetadataRequest.new(**options)
      response = Protocol::MetadataResponse.new

      @connection.request(api_key, request, response)

      response
    end

    def produce(**options)
      api_key = Protocol::PRODUCE_API_KEY
      request = Protocol::ProduceRequest.new(**options)
      response = request.requires_acks? ? Protocol::ProduceResponse.new : nil

      @connection.request(api_key, request, response)

      if request.requires_acks?
        response
      else
        nil
      end
    end
  end
end
