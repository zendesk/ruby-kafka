module Kafka
  module Protocol

    class CreateTopicsRequest
      def initialize(topics:, timeout:)
        @topics, @timeout = topics, timeout
      end

      def api_key
        CREATE_TOPICS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::CreateTopicsResponse
      end

      def encode(encoder)
        encoder.write_array(@topics) do |topic, config|
          encoder.write_string(topic)
          encoder.write_int32(config.fetch(:num_partitions))
          encoder.write_int16(config.fetch(:replication_factor))

          # Replica assignments. We don't care.
          encoder.write_array([])

          # Config entries. We don't care.
          encoder.write_array([])
        end

        # Timeout is in ms.
        encoder.write_int32(@timeout * 1000)
      end
    end

  end
end
