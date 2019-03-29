# frozen_string_literal: true

require "stringio"

module Kafka
  module Protocol
    # An encoder wraps an IO object, making it easy to write specific data types
    # to it.
    class Encoder
      # Initializes a new encoder.
      #
      # @param io [IO] an object that acts as an IO.
      def initialize(io)
        @io = io
        @io.set_encoding(Encoding::BINARY)
      end

      # Writes bytes directly to the IO object.
      #
      # @param bytes [String]
      # @return [nil]
      def write(bytes)
        @io.write(bytes)

        nil
      end

      # Writes an 8-bit boolean to the IO object.
      #
      # @param boolean [Boolean]
      # @return [nil]
      def write_boolean(boolean)
        boolean ? write_int8(1) : write_int8(0)
      end

      # Writes an 8-bit integer to the IO object.
      #
      # @param int [Integer]
      # @return [nil]
      def write_int8(int)
        write([int].pack("C"))
      end

      # Writes a 16-bit integer to the IO object.
      #
      # @param int [Integer]
      # @return [nil]
      def write_int16(int)
        write([int].pack("s>"))
      end

      # Writes a 32-bit integer to the IO object.
      #
      # @param int [Integer]
      # @return [nil]
      def write_int32(int)
        write([int].pack("l>"))
      end

      # Writes a 64-bit integer to the IO object.
      #
      # @param int [Integer]
      # @return [nil]
      def write_int64(int)
        write([int].pack("q>"))
      end

      # Writes an array to the IO object.
      #
      # Each item in the specified array will be yielded to the provided block;
      # it's the responsibility of the block to write those items using the
      # encoder.
      #
      # @param array [Array]
      # @return [nil]
      def write_array(array, &block)
        if array.nil?
          # An array can be null, which is different from it being empty.
          write_int32(-1)
        else
          write_int32(array.size)
          array.each(&block)
        end
      end

      # Writes an array to the IO object.
      # Just like #write_array, unless the size is under varint format
      #
      # @param array [Array]
      # @return [nil]
      def write_varint_array(array, &block)
        if array.nil?
          write_varint(-1)
        else
          write_varint(array.size)
          array.each(&block)
        end
      end

      # Writes a string to the IO object.
      #
      # @param string [String]
      # @return [nil]
      def write_string(string)
        if string.nil?
          write_int16(-1)
        else
          write_int16(string.bytesize)
          write(string)
        end
      end

      # Writes a string to the IO object, the size is under varint format
      #
      # @param string [String]
      # @return [nil]
      def write_varint_string(string)
        if string.nil?
          write_varint(-1)
        else
          write_varint(string.bytesize)
          write(string)
        end
      end

      # Writes an integer under varints serializing to the IO object.
      # https://developers.google.com/protocol-buffers/docs/encoding#varints
      #
      # @param int [Integer]
      # @return [nil]
      def write_varint(int)
        int = int << 1
        int = ~int | 1 if int < 0

        chunks = []
        while int >> 7 != 0
          chunks << (int & 0x7f | 0x80)
          int >>= 7
        end
        chunks << int
        write(chunks.pack("C*"))
      end

      # Writes a byte string to the IO object.
      #
      # @param bytes [String]
      # @return [nil]
      def write_bytes(bytes)
        if bytes.nil?
          write_int32(-1)
        else
          write_int32(bytes.bytesize)
          write(bytes)
        end
      end

      # Writes a byte string to the IO object, the size is under varint format
      #
      # @param bytes [String]
      # @return [nil]
      def write_varint_bytes(bytes)
        if bytes.nil?
          write_varint(-1)
        else
          write_varint(bytes.bytesize)
          write(bytes)
        end
      end

      # Encodes an object into a new buffer.
      #
      # @param object [#encode] the object that will encode itself.
      # @return [String] the encoded data.
      def self.encode_with(object)
        buffer = StringIO.new
        encoder = new(buffer)

        object.encode(encoder)

        buffer.string
      end
    end
  end
end
