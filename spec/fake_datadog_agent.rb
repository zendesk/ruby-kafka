require "socket"

class FakeDatadogAgent
  attr_reader :host, :port, :metrics

  def initialize
    @host = "127.0.0.1"
    @socket = UDPSocket.new
    @thread = nil
    @metrics = []

    @socket.bind(@host, 0)

    @port = @socket.local_address.ip_port
  end

  def start
    @thread = Thread.new { loop { receive } }
    @thread.abort_on_exception = true
  end

  def stop
    @thread.kill
  end

  def wait_for_metrics(count: 1)
    deadline = Time.now + 10

    until @metrics.count >= count || Time.now > deadline
      sleep 0.1
    end
  end

  private

  def receive
    data, sender = @socket.recvfrom(512)

    data.split("\n").each do |message|
      metric = message.split(":").first
      @metrics << metric if metric
    end
  end
end
