module Kafka
  module Protocol
    class Decoder
      def initialize(io)
        @io = io
      end

      def int8
        read(1).unpack("C").first
      end

      def int16
        read(2).unpack("s>").first
      end

      def int32
        read(4).unpack("l>").first
      end

      def int64
        read(8).unpack("q>").first
      end

      def array
        size = int32
        size.times.map { yield }
      end

      def string
        size = int16

        if size == -1
          nil
        else
          read(size)
        end
      end

      def bytes
        size = int32

        if size == -1
          nil
        else
          read(size)
        end
      end

      def read(number_of_bytes)
        @io.read(number_of_bytes)
      end
    end
  end
end
