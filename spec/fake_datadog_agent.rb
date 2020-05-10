# frozen_string_literal: true

require "socket"
require "tempfile"

class FakeDatadogAgent
  attr_reader :host, :port, :socket_path, :metrics

  def initialize
    @host = "127.0.0.1"
    @port = 9999
    @socket_path = Tempfile.open("fake_datadog_agent_sock") do |f|
      path = f.path
      f.unlink
      path
    end
    @udp_socket = UDPSocket.new
    @uds_socket = Socket.new(Socket::AF_UNIX, Socket::SOCK_DGRAM)
    @threads = []
    @metrics = []
  end

  def start
    @udp_socket.bind(@host, @port)
    @uds_socket.bind(Addrinfo.unix(@socket_path))

    @threads << Thread.new { loop { receive_from_udp_socket } }
    @threads << Thread.new { loop { receive_from_uds_socket } }
    @threads.each { |th| th.abort_on_exception = true }
  end

  def stop
    @threads.each(&:kill)
    @udp_socket.close
    @uds_socket.close
    @threads = []
  end

  def wait_for_metrics(count: 1)
    deadline = Time.now + 10

    until @metrics.count >= count || Time.now > deadline
      sleep 0.1
    end
  end

  private

  def receive_from_udp_socket
    data, _ = @udp_socket.recvfrom(512)
    add_metrics(data)
  end

  def receive_from_uds_socket
    data, _ = @uds_socket.recvfrom(512)
    add_metrics(data)
  end

  def add_metrics(data)
    data.split("\n").each do |message|
      metric = message.split(":").first
      @metrics << metric if metric
    end
  end
end
