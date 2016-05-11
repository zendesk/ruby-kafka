describe Kafka::SSLSocketWithTimeout, ".open" do
  it "times out if the server doesn't accept the connection within the timeout" do
    host = "172.16.0.0" # this address is non-routable!
    port = 4444

    timeout = 0.1
    allowed_time = timeout + 0.1

    start = Time.now

    expect {
      Kafka::SSLSocketWithTimeout.new(host, port, connect_timeout: timeout, timeout: 1, ssl_context: OpenSSL::SSL::SSLContext.new)
    }.to raise_exception(Errno::ETIMEDOUT)

    finish = Time.now

    expect(finish - start).to be < allowed_time
  end
end
