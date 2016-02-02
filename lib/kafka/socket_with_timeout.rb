require "socket"

module Kafka

  # Opens sockets in a non-blocking fashion, ensuring that we're not stalling
  # for long periods of time waiting for a connection to open.
  class SocketWithTimeout

    # Opens a socket.
    #
    # @param host [String]
    # @param port [Integer]
    # @param timeout [Integer] the connection timeout, in seconds.
    # @return [TCPSocket] the open socket.
    def initialize(host, port, timeout: nil)
      addr = Socket.getaddrinfo(host, nil)
      sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

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
        unless IO.select(nil, [@socket], nil, timeout)
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

    def read(num_bytes, timeout: nil)
      unless IO.select([@socket], nil, nil, timeout)
        raise Errno::ETIMEDOUT
      end

      @socket.read(num_bytes)
    end

    def write(bytes, timeout: nil)
      unless IO.select(nil, [@socket], nil, timeout)
        raise Errno::ETIMEDOUT
      end

      @socket.write(bytes)
    end

    def close
      @socket.close
    end

    def set_encoding(encoding)
      @socket.set_encoding(encoding)
    end
  end
end
