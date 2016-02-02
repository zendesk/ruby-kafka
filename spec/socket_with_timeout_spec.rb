describe Kafka::SocketWithTimeout, ".open" do
  it "times out if the server doesn't accept the connection within the timeout" do
    host = "10.255.255.1" # this address is non-routable!
    port = 4444

    timeout = 0.1
    allowed_time = timeout + 0.1

    start = Time.now

    expect {
      Kafka::SocketWithTimeout.new(host, port, timeout: timeout)
    }.to raise_exception(Errno::ETIMEDOUT)

    finish = Time.now

    expect(finish - start).to be < allowed_time
  end

  describe "#read" do
    it "times out after the specified amount of time" do
      host = "localhost"
      server = TCPServer.new(host, 0)
      port = server.addr[1]

      timeout = 0.1
      allowed_time = timeout + 0.1

      socket = Kafka::SocketWithTimeout.new(host, port, timeout: 1)

      start = Time.now

      expect {
        socket.read(4, timeout: timeout)
      }.to raise_exception(Errno::ETIMEDOUT)

      finish = Time.now

      expect(finish - start).to be < allowed_time
    end
  end
end
