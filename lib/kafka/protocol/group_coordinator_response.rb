module Kafka
  module Protocol
    class GroupCoordinatorResponse
      attr_reader :error_code

      attr_reader :coordinator_id, :coordinator_host, :coordinator_port

      def initialize(error_code:, coordinator_id:, coordinator_host:, coordinator_port:)
        @error_code = error_code
        @coordinator_id = coordinator_id
        @coordinator_host = coordinator_host
        @coordinator_port = coordinator_port
      end

      def self.decode(decoder)
        new(
          error_code: decoder.int16,
          coordinator_id: decoder.int32,
          coordinator_host: decoder.string,
          coordinator_port: decoder.int32,
        )
      end
    end
  end
end
