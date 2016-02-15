require "logger"
require "kafka/connection"
require "kafka/protocol"

module Kafka
  class Broker
    def self.connect(logger:, **options)
      connection = Connection.new(logger: logger, **options)
      new(connection: connection, logger: logger)
    end

    def initialize(connection:)
      @connection = connection
    end

    def to_s
      @connection.to_s
    end

    def disconnect
      @connection.close
    end

    def fetch_messages(**options)
      request = Protocol::FetchRequest.new(**options)

      @connection.send_request(request)
    end

    def list_offsets(**options)
      request = Protocol::ListOffsetRequest.new(**options)

      @connection.send_request(request)
    end

    def produce(**options)
      request = Protocol::ProduceRequest.new(**options)

      @connection.send_request(request)
    end
  end
end
