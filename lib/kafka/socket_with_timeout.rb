# frozen_string_literal: true

require "socket"

module Kafka

  # Opens sockets in a non-blocking fashion, ensuring that we're not stalling
  # for long periods of time.
  #
  # It's possible to set timeouts for connecting to the server, for reading data,
  # and for writing data. Whenever a timeout is exceeded, Errno::ETIMEDOUT is
  # raised.
  #
  class SocketWithTimeout

    # Opens a socket.
    #
    # @param host [String]
    # @param port [Integer]
    # @param connect_timeout [Integer] the connection timeout, in seconds.
    # @param timeout [Integer] the read and write timeout, in seconds.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    def initialize(host, port, connect_timeout: nil, timeout: nil)
      addr = Socket.getaddrinfo(host, nil)
      sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

      @timeout = timeout

      @socket = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        @socket.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        # IO.select will block until the socket is writable or the timeout
        # is exceeded, whichever comes first.
        unless IO.select(nil, [@socket], nil, connect_timeout)
          # IO.select returns nil when the socket is not ready before timeout
          # seconds have elapsed
          @socket.close
          raise Errno::ETIMEDOUT
        end

        begin
          # Verify there is now a good connection.
          @socket.connect_nonblock(sockaddr)
        rescue Errno::EISCONN
          # The socket is connected, we're good!
        end
      end
    end

    # Reads bytes from the socket, possible with a timeout.
    #
    # @param num_bytes [Integer] the number of bytes to read.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    # @return [String] the data that was read from the socket.
    def read(num_bytes)
      unless IO.select([@socket], nil, nil, @timeout)
        raise Errno::ETIMEDOUT
      end

      @socket.read(num_bytes)
    rescue IO::EAGAINWaitReadable
      retry
    end

    # Writes bytes to the socket, possible with a timeout.
    #
    # @param bytes [String] the data that should be written to the socket.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    # @return [Integer] the number of bytes written.
    def write(bytes)
      unless IO.select(nil, [@socket], nil, @timeout)
        raise Errno::ETIMEDOUT
      end

      @socket.write(bytes)
    end

    def close
      @socket.close
    end

    def closed?
      @socket.closed?
    end

    def set_encoding(encoding)
      @socket.set_encoding(encoding)
    end
  end
end
