module Kafka
  module Protocol
    # A wrapper around a pending operation
    class AsyncResponse
      def initialize(socket, &read_block)
        @socket = socket
        @read_proc = read_block.to_proc
      end

      def ready?
        @socket.ready?
      end

      def read_response
        @read_proc.call
      end
    end
  end
end



