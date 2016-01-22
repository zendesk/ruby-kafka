require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def initialize(host:, port:, node_id: nil, client_id:, logger:, socket_timeout: nil)
      @host, @port, @node_id = host, port, node_id

      @connection = Connection.new(
        host: host,
        port: port,
        client_id: client_id,
        socket_timeout: socket_timeout,
        logger: logger
      )

      @logger = logger
    end

    def to_s
      "#{@host}:#{@port} (node_id=#{@node_id.inspect})"
    end

    def disconnect
      @connection.close
    end

    def fetch_metadata(**options)
      api_key = Protocol::TOPIC_METADATA_API_KEY
      request = Protocol::TopicMetadataRequest.new(**options)
      response_class = Protocol::MetadataResponse

      response = @connection.request(api_key, request, response_class)

      response.topics.each do |topic|
        Protocol.handle_error(topic.topic_error_code)

        topic.partitions.each do |partition|
          Protocol.handle_error(partition.partition_error_code)
        end
      end

      response
    end

    def produce(**options)
      api_key = Protocol::PRODUCE_API_KEY
      request = Protocol::ProduceRequest.new(**options)
      response_class = request.requires_acks? ? Protocol::ProduceResponse : nil

      @connection.request(api_key, request, response_class)
    end
  end
end
