# frozen_string_literal: true

require "fake_server"
require "timeout"
require "json"

describe "Client reconnect backoff over the original connection stack" do
  let(:server) { TCPServer.new("127.0.0.1", 0) }
  let(:port) { server.addr[1] }
  let(:options) { {} }
  let(:client) do
    Kafka.new(
      ["127.0.0.1:#{port}"],
      logger: LOGGER,
      connect_timeout: 0.1,
      socket_timeout: 0.1,
      reconnect_backoff: 0.1,
      reconnect_backoff_max: 0.4,
      **options
    )
  end

  let(:builder) { client.instance_variable_get(:@connection_builder) }
  let(:backoff) { builder.instance_variable_get(:@reconnect_backoff) }
  let(:pool) { Kafka::BrokerPool.new(connection_builder: builder, logger: LOGGER) }
  let(:broker) { pool.connect("127.0.0.1", port, node_id: 1) }
  let(:handler) { FakeServer.new(server) }
  let(:waits) { [] }

  before do
    @now = 0.0
    @refuse = false
    @sockets = []
    @servers = []
    @opens = []
    allow(backoff).to receive(:monotonic_now) { @now }
    allow(backoff).to receive(:rand).and_return(0.5)
    allow(backoff).to receive(:sleep) { |duration| waits << duration; @now += duration }
    allow(Kafka::SocketWithTimeout).to receive(:new).and_wrap_original do |original, host, target_port, **args|
      @opens << Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise Errno::ECONNREFUSED if @refuse
      socket = original.call(host, target_port, **args)
      @sockets << socket
      socket
    end
  end

  after do
    @servers.each { |thread| thread.kill; thread.join }
    @sockets.each { |socket| socket.close unless socket.closed? }
    server.close unless server.closed?
  end

  def start_server
    @servers << Thread.new { handler.start }
  end

  def refuse(target = broker)
    @refuse = true
    expect { target.api_versions }.to raise_error(Kafka::ConnectionError)
  ensure
    @refuse = false
  end

  it "opens lazily on the first operation and reuses a healthy connection" do
    expect(@opens).to be_empty
    start_server
    2.times { expect(broker.api_versions.error_code).to eq(0) }

    expect(@opens.length).to eq(1)
    expect(waits).to be_empty
  end

  context "with healthy cold connections" do
    let(:options) { { socket_timeout: 5 } }

    it "lets another pool exchange a response while an earlier Broker request is still waiting" do
      entered, release = Queue.new, Queue.new
      allow(handler).to receive(:handle_client).and_wrap_original do |original, socket|
        entered << true
        release.pop
        original.call(socket)
      end

      begin
        start_server
        first = Thread.new { broker.api_versions }
        Timeout.timeout(3) { entered.pop }
        other_handler = FakeServer.new(server)
        @servers << Thread.new { other_handler.start }
        other_pool = Kafka::BrokerPool.new(connection_builder: builder, logger: LOGGER)
        other_broker = other_pool.connect("127.0.0.1", port, node_id: 2)

        response = Timeout.timeout(3) { other_broker.api_versions }
        expect(response.error_code).to eq(0)
        expect(first).to be_alive
        expect(@opens.length).to eq(2)
        expect(waits).to be_empty
      ensure
        release << true
        if first
          expect(first.join(3)).to equal(first)
          expect(first.value.error_code).to eq(0)
        end
      end
    end
  end

  it "shares failure history with freshly recreated metadata brokers and producer pools" do
    @refuse = true
    producer = client.producer(retry_backoff: 1, max_retries: 2)
    producer.produce("accepted-id", topic: "events", partition: 0)
    3.times do
      expect { producer.deliver_messages }.to raise_error(Kafka::Error)
      expect(producer.buffer_size).to eq(1)
    end
    expect { client.topics }.to raise_error(Kafka::ConnectionError)

    expect(@opens.length).to eq(5)
    expect(waits).to match([
      a_value_within(0.000001).of(0.1),
      a_value_within(0.000001).of(0.2),
      a_value_within(0.000001).of(0.4),
      a_value_within(0.000001).of(0.4),
    ])
  end

  it "does not gate a healthy socket when another connection to its endpoint fails" do
    start_server
    broker.api_versions
    refuse(pool.connect("127.0.0.1", port))
    expect(broker.api_versions.error_code).to eq(0)

    expect(waits).to be_empty
    expect(@opens.length).to eq(2)
  end

  it "gates the reopen of a closed but non-nil connection" do
    start_server
    broker.api_versions
    existing = broker.instance_variable_get(:@connection)
    broker.disconnect
    @servers.each { |thread| thread.kill; thread.join }
    refuse(pool.connect("127.0.0.1", port))
    start_server
    expect(broker.api_versions.error_code).to eq(0)

    expect(broker.instance_variable_get(:@connection)).to equal(existing)
    expect(waits).to eq([0.1])
  end

  it "arms cold backoff after a real existing-socket read failure" do
    start_server
    broker.api_versions
    @servers.each { |thread| thread.kill; thread.join }
    expect { broker.api_versions }.to raise_error(Kafka::ConnectionError)
    start_server
    expect(broker.api_versions.error_code).to eq(0)

    expect(waits).to eq([0.1])
  end

  it "preserves the native IdleConnection close-and-retry path without recording a failure" do
    start_server
    broker.api_versions
    connection = broker.instance_variable_get(:@connection)
    connection.instance_variable_set(:@last_request, Time.now - Kafka::Connection::IDLE_TIMEOUT - 1)
    start_server
    expect(broker.api_versions.error_code).to eq(0)

    expect(@opens.length).to eq(2)
    expect(waits).to be_empty
  end

  it "resets after decoding a correlated Kafka error response" do
    2.times { refuse }
    handler.api_versions_error_code = 17
    start_server
    expect(broker.api_versions.error_code).to eq(17)
    refuse(pool.connect("127.0.0.1", port))
    @now += 0.04
    refuse(pool.connect("127.0.0.1", port))

    expect(waits.length).to eq(3)
    expect(waits.last).to be_within(0.000001).of(0.06)
  end

  it "resets after a completed acks=0 write without waiting for a response" do
    2.times { refuse }
    start_server
    expect(broker.produce(
      required_acks: 0,
      timeout: 1,
      messages_for_topics: {},
      compressor: Kafka::Compressor.new(instrumenter: Kafka::Instrumenter.new),
    )).to be_nil
    refuse(pool.connect("127.0.0.1", port))
    @now += 0.04
    refuse(pool.connect("127.0.0.1", port))

    expect(waits.length).to eq(3)
    expect(waits.last).to be_within(0.000001).of(0.06)
  end

  context "with SASL PLAIN" do
    let(:options) do
      {
        sasl_over_ssl: false,
        sasl_plain_username: "spec_username",
        sasl_plain_password: "spec_password",
      }
    end

    it "waits before authentication and resets only after the application exchange" do
      2.times { refuse }
      start_server
      expect(broker.api_versions.error_code).to eq(0)
      refuse(pool.connect("127.0.0.1", port))
      @now += 0.04
      refuse(pool.connect("127.0.0.1", port))

      expect(waits.length).to eq(3)
      expect(waits.last).to be_within(0.000001).of(0.06)
    end

    it "does not reset on successful TCP and SASL followed by failed application I/O" do
      refuse
      authenticated = []
      authenticator = builder.instance_variable_get(:@sasl_authenticator)
      allow(authenticator).to receive(:authenticate!).and_wrap_original do |original, connection|
        original.call(connection)
        authenticated << connection.open?
      end
      allow(handler).to receive(:auth).and_wrap_original do |original, *args|
        original.call(*args)
        raise EOFError
      end
      start_server
      expect { broker.api_versions }.to raise_error(Kafka::ConnectionError)
      expect(authenticated).to eq([true])
      refuse

      expect(waits).to match([0.1, a_value_within(0.000001).of(0.2)])
    end

    it "counts a real post-handshake EOF wrapped by the existing SASL implementation" do
      allow(handler).to receive(:auth) do |_mechanism, _encoder, decoder|
        decoder.bytes
        raise EOFError
      end
      start_server
      expect { broker.api_versions }.to raise_error(Kafka::Error) { |error|
        expect(error.message).to match(/SASL PLAIN authentication failed/)
        expect(error.cause).to be_a(EOFError)
      }
      refuse

      expect(waits).to eq([0.1])
    end
  end

  context "with a permanent SASL SCRAM rejection" do
    let(:options) do
      {
        sasl_over_ssl: false,
        sasl_scram_username: "spec_username",
        sasl_scram_password: "wrong",
        sasl_scram_mechanism: "sha256",
      }
    end

    it "preserves the rejection and does not introduce authentication retries or cooldown" do
      start_server
      expect { broker.api_versions }.to raise_error(Kafka::FailedScramAuthentication)
      expect(@opens.length).to eq(1)
      refuse

      expect(waits).to be_empty
    end
  end

  it "spaces real refused TCP opens across metadata broker replacement without dropping buffered data" do
    allow(backoff).to receive(:monotonic_now).and_call_original
    allow(backoff).to receive(:sleep).and_call_original
    server.close
    producer = client.producer(retry_backoff: 1, max_retries: 2)
    producer.produce("accepted-id", topic: "events", partition: 0)
    errors = []

    3.times do
      expect { producer.deliver_messages }.to raise_error(Kafka::Error) { |error|
        errors << error.class
        if error.is_a?(Kafka::DeliveryFailed)
          expect(error.failed_messages.map(&:value)).to eq(["accepted-id"])
        end
      }
      expect(producer.buffer_size).to eq(1)
    end

    expect(errors).to eq([Kafka::ConnectionError, Kafka::DeliveryFailed, Kafka::DeliveryFailed])
    expect(@opens.length).to eq(3)
    expect(@opens[1] - @opens[0]).to be >= 0.095
    expect(@opens[2] - @opens[1]).to be >= 0.195
  end
end

describe "A lazily initialized client inherited across fork" do
  it "opens an original Connection in the child and decodes a real FakeServer response" do
    server = TCPServer.new("127.0.0.1", 0)
    client = Kafka.new(
      ["127.0.0.1:#{server.addr[1]}"],
      logger: LOGGER,
      connect_timeout: 1,
      socket_timeout: 1,
      reconnect_backoff: 0.1,
      reconnect_backoff_max: 0.2,
    )
    builder = client.instance_variable_get(:@connection_builder)
    broker = Kafka::Broker.new(connection_builder: builder, host: "127.0.0.1", port: server.addr[1], logger: LOGGER)
    fake_server = FakeServer.start(server)
    reader, writer = IO.pipe
    pid = nil
    begin
      expect(broker).not_to be_connected
      pid = fork do
        reader.close
        result = begin
          response = broker.api_versions
          { error_code: response.error_code, open: broker.instance_variable_get(:@connection).open? }
        rescue StandardError => error
          { error: [error.class.name, error.message] }
        end
        broker.disconnect
        writer.write(JSON.generate(result))
        writer.close
        exit! 0
      end
      writer.close
      result = Timeout.timeout(3) { JSON.parse(reader.read, symbolize_names: true) }
      Process.waitpid(pid)
      pid = nil
      expect(result).to eq(error_code: 0, open: true)
      expect(broker).not_to be_connected
    ensure
      if pid
        Process.kill("KILL", pid)
        Process.waitpid(pid)
      end
      reader.close unless reader.closed?
      writer.close unless writer.closed?
      fake_server.kill
      fake_server.join
      client.close
      server.close
    end
  end
end
