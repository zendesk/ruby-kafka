module Kafka
  module Protocol
    class Encoder
      def initialize(io)
        @io = io
        @io.set_encoding(Encoding::BINARY)
      end

      def write(bytes)
        @io.write(bytes)
      end

      def write_int8(int)
        write([int].pack("C"))
      end

      def write_int16(int)
        write([int].pack("s>"))
      end

      def write_int32(int)
        write([int].pack("l>"))
      end

      def write_int64(int)
        write([int].pack("q>"))
      end

      def write_array(array, &block)
        write_int32(array.size)
        array.each(&block)
      end

      def write_string(string)
        if string.nil?
          write_int16(-1)
        else
          write_int16(string.bytesize)
          write(string)
        end
      end

      def write_bytes(bytes)
        if bytes.nil?
          write_int32(-1)
        else
          write_int32(bytes.bytesize)
          write(bytes)
        end
      end

      def self.encode_with(object)
        buffer = StringIO.new
        encoder = new(buffer)

        object.encode(encoder)

        buffer.string
      end
    end
  end
end
