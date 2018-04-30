# frozen_string_literal: true

module Kafka
  module Protocol

    class CreatePartitionsRequest
      def initialize(topics:, timeout:)
        @topics, @timeout = topics, timeout
      end

      def api_key
        CREATE_PARTITIONS_API
      end

      def api_version
        0
      end

      def response_class
        Protocol::CreatePartitionsResponse
      end

      def encode(encoder)
        encoder.write_array(@topics) do |topic, count, assignments|
          encoder.write_string(topic)
          encoder.write_int32(count)
          encoder.write_array(assignments) do |assignment|
            encoder.write_array(assignment) do |broker|
              encoder.write_int32(broker)
            end
          end
        end
        # Timeout is in ms.
        encoder.write_int32(@timeout * 1000)
        # validate_only. There isn't any use case for this in real life. So
        # let's ignore it for now
        encoder.write_boolean(false)
      end
    end

  end
end
