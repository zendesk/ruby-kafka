module Kafka
  module Protocol
    class ConsumerGroupProtocol
      def initialize(name:, metadata:)
        @name = name
        @metadata = metadata
      end
    end
  end
end
