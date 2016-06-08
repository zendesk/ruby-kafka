require "socket"

class FakeDatadogAgent
  attr_reader :host, :port, :metrics

  def initialize
    @host = "127.0.0.1"
    @port = 9999
    @socket = UDPSocket.new
    @thread = nil
    @metrics = []

    @socket.bind(@host, @port)
  end

  def start
    @thread = Thread.new { loop { receive } }
    @thread.abort_on_exception = true
  end

  def wait_for_metrics
    sleep 0.1 until @metrics.count > 0
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
