require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def initialize(connection_builder:, host:, port:, node_id: nil, logger:)
      @connection_builder = connection_builder
      @connection = nil
      @host = host
      @port = port
      @node_id = node_id
      @logger = logger
    end

    def address_match?(host, port)
      host == @host && port == @port
    end

    # @return [String]
    def to_s
      "#{connection} (node_id=#{@node_id.inspect})"
    end

    # @return [nil]
    def disconnect
      connection.close
    end

    # Fetches cluster metadata from the broker.
    #
    # @param (see Kafka::Protocol::TopicMetadataRequest#initialize)
    # @return [Kafka::Protocol::MetadataResponse]
    def fetch_metadata(**options)
      request = Protocol::TopicMetadataRequest.new(**options)

      send_request(request)
    end

    # Fetches messages from a specified topic and partition.
    #
    # @param (see Kafka::Protocol::FetchRequest#initialize)
    # @return [Kafka::Protocol::FetchResponse]
    def fetch_messages(**options)
      request = Protocol::FetchRequest.new(**options)

      send_request(request)
    end

    # Lists the offset of the specified topics and partitions.
    #
    # @param (see Kafka::Protocol::ListOffsetRequest#initialize)
    # @return [Kafka::Protocol::ListOffsetResponse]
    def list_offsets(**options)
      request = Protocol::ListOffsetRequest.new(**options)

      send_request(request)
    end

    # Produces a set of messages to the broker.
    #
    # @param (see Kafka::Protocol::ProduceRequest#initialize)
    # @return [Kafka::Protocol::ProduceResponse]
    def produce(**options)
      request = Protocol::ProduceRequest.new(**options)

      send_request(request)
    end

    def fetch_offsets(**options)
      request = Protocol::OffsetFetchRequest.new(**options)

      send_request(request)
    end

    def commit_offsets(**options)
      request = Protocol::OffsetCommitRequest.new(**options)

      send_request(request)
    end

    def join_group(**options)
      request = Protocol::JoinGroupRequest.new(**options)

      send_request(request)
    end

    def sync_group(**options)
      request = Protocol::SyncGroupRequest.new(**options)

      send_request(request)
    end

    def leave_group(**options)
      request = Protocol::LeaveGroupRequest.new(**options)

      send_request(request)
    end

    def find_group_coordinator(**options)
      request = Protocol::GroupCoordinatorRequest.new(**options)

      send_request(request)
    end

    def heartbeat(**options)
      request = Protocol::HeartbeatRequest.new(**options)

      send_request(request)
    end

    def create_topics(**options)
      request = Protocol::CreateTopicsRequest.new(**options)

      send_request(request)
    end

    def api_versions
      request = Protocol::ApiVersionsRequest.new

      send_request(request)
    end

    private

    def send_request(request)
      connection.send_request(request)
    rescue IdleConnection
      @logger.warn "Connection has been unused for too long, re-connecting..."
      @connection.close rescue nil
      @connection = nil
      retry
    rescue ConnectionError
      @connection.close rescue nil
      @connection = nil

      raise
    end

    def connection
      @connection ||= @connection_builder.build_connection(@host, @port)
    end
  end
end
