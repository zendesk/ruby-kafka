describe Kafka::SocketWithTimeout, ".open" do
  it "times out if the server doesn't accept the connection within the timeout" do
    host = "10.255.255.1" # this address is non-routable!
    port = 4444

    timeout = 0.1
    allowed_time = timeout + 0.1

    start = Time.now

    expect {
      Kafka::SocketWithTimeout.open(host, port, timeout: timeout)
    }.to raise_exception(Errno::ETIMEDOUT)

    finish = Time.now

    expect(finish - start).to be < allowed_time
  end
end
