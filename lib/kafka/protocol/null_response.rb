module Kafka
  module Protocol

    # A response class used when no response is expected.
    class NullResponse
      def self.decode(decoder)
        nil
      end
    end
  end
end
