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

    def to_s
      "#{@connection} (node_id=#{@node_id.inspect})"
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
          begin
            Protocol.handle_error(partition.partition_error_code)
          rescue ReplicaNotAvailable
            # This error can be safely ignored per the protocol specification.
            @logger.warn "Replica not available for topic #{topic.topic_name}, partition #{partition.partition_id}"
          end
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
