# frozen_string_literal: true

module Kafka
  module Protocol
    # A decoder wraps an IO object, making it easy to read specific data types
    # from it. The Kafka protocol is not self-describing, so a client must call
    # these methods in just the right order for things to work.
    class Decoder
      def self.from_string(str)
        new(StringIO.new(str))
      end

      # Initializes a new decoder.
      #
      # @param io [IO] an object that acts as an IO.
      def initialize(io)
        @io = io
      end

      def eof?
        @io.eof?
      end

      # Get some next bytes without touching the current io offset
      #
      # @return [Integer]
      def peek(offset, length)
        data = @io.read(offset + length)
        return [] if data.nil?
        @io.ungetc(data)
        data.bytes[offset, offset + length] || []
      end

      # Decodes an 8-bit boolean from the IO object.
      #
      # @return [Boolean]
      def boolean
        read(1) == 0x1
      end

      # Decodes an 8-bit integer from the IO object.
      #
      # @return [Integer]
      def int8
        read(1).unpack("C").first
      end

      # Decodes a 16-bit integer from the IO object.
      #
      # @return [Integer]
      def int16
        read(2).unpack("s>").first
      end

      # Decodes a 32-bit integer from the IO object.
      #
      # @return [Integer]
      def int32
        read(4).unpack("l>").first
      end

      # Decodes a 64-bit integer from the IO object.
      #
      # @return [Integer]
      def int64
        read(8).unpack("q>").first
      end

      # Decodes an array from the IO object.
      #
      # The provided block will be called once for each item in the array. It is
      # the responsibility of the block to decode the proper type in the block,
      # since there's no information that allows the type to be inferred
      # automatically.
      #
      # @return [Array]
      def array(&block)
        size = int32
        size.times.map(&block)
      end

      # Decodes an array from the IO object.
      # Just like #array except the size is in varint format
      #
      # @return [Array]
      def varint_array(&block)
        size = varint
        size.times.map(&block)
      end

      # Decodes a string from the IO object.
      #
      # @return [String]
      def string
        size = int16

        if size == -1
          nil
        else
          read(size)
        end
      end

      # Decodes a string from the IO object, the size is in varint format
      #
      # @return [String]
      def varint_string
        size = varint

        if size == -1
          nil
        else
          read(size)
        end
      end

      # Read an integer under varints serializing from the IO object.
      # https://developers.google.com/protocol-buffers/docs/encoding#varints
      #
      # @return [Integer]
      def varint
        group = 0
        data = 0
        while (chunk = int8) & 0x80 != 0
          data |= (chunk & 0x7f) << group
          group += 7
        end
        data |= chunk << group
        data & 0b1 != 0 ? ~(data >> 1) : (data >> 1)
      end

      # Decodes a list of bytes from the IO object.
      #
      # @return [String]
      def bytes
        size = int32

        if size == -1
          nil
        else
          read(size)
        end
      end

      # Decodes a list of bytes from the IO object. The size is in varint format
      #
      # @return [String]
      def varint_bytes
        size =  varint

        if size == -1
          nil
        else
          read(size)
        end
      end

      # Reads the specified number of bytes from the IO object, returning them
      # as a String.
      #
      # @return [String]
      def read(number_of_bytes)
        return "" if number_of_bytes == 0

        data = @io.read(number_of_bytes) or raise EOFError

        # If the `read` call returned less data than expected we should not
        # proceed.
        raise EOFError if data.size != number_of_bytes

        data
      end
    end
  end
end
