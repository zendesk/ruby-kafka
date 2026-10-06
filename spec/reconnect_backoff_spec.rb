# frozen_string_literal: true

require "timeout"
require "json"

describe "Kafka::ReconnectBackoff" do
  let(:backoff) { Kafka::ReconnectBackoff.new(0.1, 0.5) }
  let(:open_connection) { instance_double(Kafka::Connection, open?: true) }
  let(:closed_connection) { instance_double(Kafka::Connection, open?: false) }
  let(:waits) { [] }

  before do
    @now = 0.0
    @threads = []
    allow(backoff).to receive(:monotonic_now) { @now }
    allow(backoff).to receive(:rand).and_return(0.5)
    allow(backoff).to receive(:sleep) do |duration|
      expect(backoff.instance_variable_get(:@mutex).owned?).to eq(false)
      waits << duration
      @now += duration
    end
  end

  after do
    @threads.each do |thread|
      thread.kill
      thread.join
    end
  end

  def attempt(connection = nil, host = "broker", port = 9092, &block)
    backoff.with_attempt(host, port, connection, &block)
  end

  def fail_attempt(connection = nil, host = "broker", port = 9092, error = Kafka::ConnectionError.new("offline"))
    expect { attempt(connection, host, port) { raise error } }.to raise_error { |actual|
      expect(actual).to equal(error)
    }
  end

  def start_thread(&block)
    @threads << Thread.new(&block)
    @threads.last
  end

  def take(queue)
    Timeout.timeout(3) { queue.pop }
  end

  def wait_until(&block)
    Timeout.timeout(3) { Thread.pass until block.call }
  end

  def entries
    backoff.instance_variable_get(:@entries)
  end

  def in_child
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      result = begin
        yield
      rescue StandardError => error
        { error: [error.class.name, error.message] }
      end
      writer.write(JSON.generate(result))
      writer.close
      exit! 0
    end
    writer.close
    result = Timeout.timeout(3) { JSON.parse(reader.read, symbolize_names: true) }
    Process.waitpid(pid)
    pid = nil
    result
  ensure
    if pid
      Process.kill("KILL", pid)
      Process.waitpid(pid)
    end
    reader.close unless reader.closed?
    writer.close unless writer.closed?
  end

  it "executes the first cold operation once, immediately and outside the mutex" do
    calls = 0
    result = attempt do
      expect(backoff.instance_variable_get(:@mutex).owned?).to eq(false)
      calls += 1
      :response
    end

    expect(result).to eq(:response)
    expect(calls).to eq(1)
    expect(waits).to be_empty
    expect(entries).to be_empty
  end

  it "doubles the unjittered delay, applies bounded jitter, and caps the final wait" do
    allow(backoff).to receive(:rand).and_return(0.0, 0.5, 1.0, 0.0, 1.0, 0.5)
    6.times { fail_attempt }

    [0.08, 0.2, 0.48, 0.4, 0.5].zip(waits) do |expected, actual|
      expect(actual).to be_within(0.000001).of(expected)
    end
    expect(waits.length).to eq(5)
  end

  it "stays capped through a long failure streak" do
    1100.times { fail_attempt }

    expect(waits.last).to be_within(0.000001).of(0.5)
    expect(waits.max).to be_within(0.000001).of(0.5)
  end

  it "waits only the remaining deadline, including time spent in native retry sleeps" do
    fail_attempt
    @now += 0.04
    fail_attempt
    @now += 0.3
    attempt { :response }

    expect(waits.length).to eq(1)
    expect(waits.first).to be_within(0.000001).of(0.06)
  end

  it "rechecks the deadline after an early wake without replaying the operation" do
    fail_attempt
    allow(backoff).to receive(:sleep) do |duration|
      waits << duration
      @now += waits.length == 1 ? duration / 2 : duration
    end
    calls = 0
    attempt { calls += 1 }

    expect(waits).to eq([0.1, 0.05])
    expect(calls).to eq(1)
  end

  it "starts the deadline when a slow failed operation finishes" do
    expect { attempt { @now += 5; raise EOFError } }.to raise_error(EOFError)
    attempt { :response }

    expect(waits.first).to be_within(0.000001).of(0.1)
  end

  it "never delays an existing open socket or lets its success reset cold failures" do
    fail_attempt
    expect(attempt(open_connection) { :response }).to eq(:response)
    expect(waits).to be_empty
    attempt { :response }

    expect(waits).to eq([0.1])
  end

  it "gates a non-nil connection whose socket is closed" do
    fail_attempt
    attempt(closed_connection) { :response }

    expect(waits).to eq([0.1])
  end

  it "resets only after a completed cold operation" do
    2.times { fail_attempt }
    attempt { :response }
    fail_attempt
    attempt { :response }

    expect(waits).to match([
      a_value_within(0.000001).of(0.1),
      a_value_within(0.000001).of(0.2),
      a_value_within(0.000001).of(0.1),
    ])
  end

  [Kafka::ConnectionError, SocketError, Errno::ECONNRESET, Errno::ETIMEDOUT, EOFError, IOError].each do |error_class|
    it "preserves #{error_class} and arms backoff for an existing socket failure" do
      fail_attempt(open_connection, "broker", 9092, error_class.new("transport"))
      attempt { :response }

      expect(waits).to eq([0.1])
    end
  end

  it "recognizes a transport cause wrapped by SASL without replacing the exception" do
    wrapped = begin
      begin
        raise EOFError, "auth read"
      rescue EOFError
        raise Kafka::Error, "SASL PLAIN authentication failed"
      end
    rescue Kafka::Error => error
      error
    end
    fail_attempt(nil, "broker", 9092, wrapped)
    attempt { :response }

    expect(waits).to eq([0.1])
  end

  [ArgumentError, TypeError, NoMethodError, Kafka::Error, Kafka::IdleConnection,
   Kafka::UnsupportedSaslMechanism, Kafka::NetworkException, OpenSSL::SSL::SSLError].each do |error_class|
    it "does not classify #{error_class} as a transport failure" do
      fail_attempt(nil, "broker", 9092, error_class.new("not transport"))
      attempt { :response }

      expect(waits).to be_empty
    end
  end

  it "neither increments nor clears history on a programming or authentication rejection" do
    fail_attempt
    fail_attempt(nil, "broker", 9092, ArgumentError.new("encoding"))
    fail_attempt
    attempt { :response }

    expect(waits).to match([0.1, a_value_within(0.000001).of(0.2)])
  end

  it "keeps textual endpoints and clients independent" do
    fail_attempt
    attempt(nil, "alias", 9092) { :response }
    attempt(nil, "broker", 9093) { :response }
    other = Kafka::ReconnectBackoff.new(0.1, 0.5)
    expect(other).not_to receive(:sleep)
    other.with_attempt("broker", 9092, nil) { :response }
    expect(waits).to be_empty
    attempt { :response }
    expect(waits).to eq([0.1])
  end

  it "owns an immutable copy of the textual endpoint" do
    host = "broker".dup
    fail_attempt(nil, host, 9092)
    host.replace("changed")
    attempt(nil, "broker".dup, "9092") { :response }

    expect(waits).to eq([0.1])
  end

  it "admits pristine cold operations in parallel while an earlier operation is still running" do
    entered, release, following = Queue.new, Queue.new, Queue.new
    begin
      first = start_thread { attempt { entered << true; release.pop } }
      take(entered)
      second = start_thread { attempt { following << :response } }

      expect(take(following)).to eq(:response)
      expect(first).to be_alive
      expect(waits).to be_empty
      release << true
      first.join
      second.join
      expect(entries).to be_empty
    ensure
      release << true
    end
  end

  it "admits one failed-endpoint probe but never blocks healthy sockets or unrelated endpoints" do
    fail_attempt
    entered, release, following = Queue.new, Queue.new, Queue.new
    first = start_thread { attempt { entered << true; release.pop } }
    take(entered)
    second = start_thread { attempt { following << true } }
    wait_until { entries.values.first.observers == 2 }
    expect(following).to be_empty
    expect(attempt(open_connection) { :live }).to eq(:live)
    expect(attempt(nil, "other") { :independent }).to eq(:independent)
    expect(following).to be_empty
    release << true
    take(following)
    first.join
    second.join
    expect(entries).to be_empty
  end

  it "rechecks failure history so waiting cold operations run in parallel after recovery" do
    fail_attempt
    entered, release, following, finish = Queue.new, Queue.new, Queue.new, Queue.new
    begin
      probe = start_thread { attempt { entered << true; release.pop; :recovered } }
      take(entered)
      waiters = 2.times.map do
        start_thread { attempt { following << :response; finish.pop } }
      end
      wait_until { entries.values.first.observers == 3 }
      expect(following).to be_empty
      release << true
      probe.join

      2.times { expect(take(following)).to eq(:response) }
      expect(entries.values.first.probing).to eq(false)
      2.times { finish << true }
      waiters.each(&:join)
      expect(entries).to be_empty
    ensure
      release << true
      2.times { finish << true }
    end
  end

  it "captures pristine admission generation before a later failure can be recorded" do
    admitted, release = Queue.new, Queue.new
    allow(backoff).to receive(:claim_probe).and_wrap_original do |original, entry|
      result = original.call(entry)
      admitted << entry
      release.pop
      result
    end

    begin
      older = start_thread { attempt { :response } }
      entry = take(admitted)
      fail_attempt(open_connection)
      release << true
      expect(older.join(3)).to equal(older)
      allow(backoff).to receive(:claim_probe).and_call_original

      expect(entry.backoff).to eq(0.1)
      expect(waits).to be_empty
      attempt { :response }
      expect(waits).to eq([0.1])
      expect(entries).to be_empty
    ensure
      release << true
    end
  end

  it "does not let an older cold success erase a concurrent newer socket failure" do
    entered, release = Queue.new, Queue.new
    older = start_thread { attempt { entered << true; release.pop; :response } }
    take(entered)
    fail_attempt(open_connection)
    release << true
    older.join
    attempt { :response }

    expect(waits).to eq([0.1])
  end

  it "accounts for a newer socket failure while the cold probe is sleeping" do
    fail_attempt
    sleeping, release = Queue.new, Queue.new
    allow(backoff).to receive(:sleep) do |duration|
      waits << duration
      if waits.length == 1
        sleeping << true
        release.pop
      end
      @now += duration
    end
    probe = start_thread { attempt { :response } }
    take(sleeping)
    @now = 0.05
    fail_attempt(open_connection)
    release << true
    probe.join

    expect(waits.length).to eq(2)
    expect(waits.last).to be_within(0.000001).of(0.1)
    expect(entries).to be_empty
  end

  [:checkout, :claim_probe].each do |transition|
    it "defers a kill until #{transition} ownership is recorded" do
      fail_attempt
      entry = entries.values.first
      entered, release = Queue.new, Queue.new
      calls = 0
      allow(backoff).to receive(transition).and_wrap_original do |original, *args|
        result = original.call(*args)
        entered << true
        release.pop
        result
      end

      begin
        probe = start_thread { attempt { calls += 1 } }
        take(entered)
        probe.kill
        release << true
        expect(probe.join(3)).to equal(probe)
        allow(backoff).to receive(transition).and_call_original

        expect(calls).to eq(0)
        expect(entry.probing).to eq(false)
        expect(entry.observers).to eq(0)
        expect(Timeout.timeout(3) { attempt { :response } }).to eq(:response)
        expect(waits).to eq([0.1])
        expect(entries).to be_empty
      ensure
        release << true
      end
    end
  end

  it "finishes registry cleanup when killed between releasing the probe and its observer" do
    fail_attempt
    entered, finish, cleaning, release = Queue.new, Queue.new, Queue.new, Queue.new
    begin
      probe = start_thread { attempt { entered << true; finish.pop; :response } }
      take(entered)
      entry = entries.values.first
      allow(entry).to receive(:probing=).and_wrap_original do |original, value|
        result = original.call(value)
        cleaning << true
        release.pop
        result
      end

      finish << true
      take(cleaning)
      probe.kill
      release << true
      expect(probe.join(3)).to equal(probe)
      allow(entry).to receive(:probing=).and_call_original

      expect(entry.probing).to eq(false)
      expect(entry.observers).to eq(0)
      expect(entries).to be_empty
      expect(Timeout.timeout(3) { attempt { :response } }).to eq(:response)
    ensure
      finish << true
      release << true
    end
  end

  it "releases a killed probe and wakes its waiter" do
    fail_attempt
    entered, following = Queue.new, Queue.new
    probe = start_thread { attempt { entered << true; Queue.new.pop } }
    take(entered)
    waiter = start_thread { attempt { following << true } }
    wait_until { entries.values.first.observers == 2 }
    probe.kill
    probe.join
    take(following)
    waiter.join

    expect(entries).to be_empty
  end

  it "releases a killed waiter without releasing somebody else's probe" do
    fail_attempt
    entered, release = Queue.new, Queue.new
    probe = start_thread { attempt { entered << true; release.pop } }
    take(entered)
    waiter = start_thread { attempt { raise "cancelled waiter ran" } }
    wait_until { entries.values.first.observers == 2 }
    waiter.kill
    waiter.join
    expect(entries.values.first.observers).to eq(1)
    expect(entries.values.first.probing).to eq(true)
    release << true
    probe.join

    expect(entries).to be_empty
  end

  it "releases a probe interrupted during cooldown without inventing a failure" do
    fail_attempt
    sleeping = Queue.new
    allow(backoff).to receive(:sleep) { sleeping << true; Queue.new.pop }
    probe = start_thread { attempt { raise "cancelled operation ran" } }
    take(sleeping)
    probe.kill
    probe.join
    @now = 0.1
    fail_attempt
    expect(entries.values.first.backoff).to eq(0.2)
    expect(entries.values.first.observers).to eq(0)
  end

  it "releases on nonlocal exits and exceptions outside StandardError" do
    expect(catch(:cancel) { attempt { throw :cancel, :cancelled } }).to eq(:cancelled)
    expect { attempt { raise Interrupt } }.to raise_error(Interrupt)
    expect(attempt { :response }).to eq(:response)
    expect(entries).to be_empty
  end

  it "prunes idle failures without a global capacity gate or retained error objects" do
    1025.times { |index| fail_attempt(nil, "broker-#{index}") }
    expect(entries.size).to eq(1025)
    expect(waits).to be_empty
    expect(entries.values.first.to_a).to all(satisfy { |value|
      value.is_a?(Numeric) || value == true || value == false || value.is_a?(ConditionVariable)
    })
    @now = 301
    fail_attempt(nil, "new-broker")

    expect(entries.size).to eq(1)
    expect(waits).to be_empty
  end

  it "retains idle failures for at least the configured maximum backoff" do
    large = Kafka::ReconnectBackoff.new(1, 600)
    allow(large).to receive(:monotonic_now) { @now }
    expect { large.with_attempt("broker", 9092, nil) { raise EOFError } }.to raise_error(EOFError)
    @now = 301
    large.with_attempt("other", 9092, nil) { :response }
    expect(large.instance_variable_get(:@entries).size).to eq(1)
    @now = 601
    large.with_attempt("other", 9092, nil) { :response }
    expect(large.instance_variable_get(:@entries)).to be_empty
  end

  it "never prunes active probes or their waiters" do
    fail_attempt
    entered, release, following = Queue.new, Queue.new, Queue.new
    probe = start_thread { attempt { entered << true; release.pop } }
    take(entered)
    waiter = start_thread { attempt { following << true } }
    wait_until { entries.values.first.observers == 2 }
    @now = 1000
    attempt(nil, "other") { :response }
    expect(entries.values.first.observers).to eq(2)
    expect(following).to be_empty
    release << true
    take(following)
    probe.join
    waiter.join
  end

  it "resets child history while another parent thread still owns the inherited mutex" do
    fail_attempt
    entry = entries.values.first
    mutex = backoff.instance_variable_get(:@mutex)
    entered, release = Queue.new, Queue.new
    owner = start_thread { mutex.synchronize { entered << true; release.pop } }
    take(entered)
    begin
      result = in_child do
        response = attempt { "response" }
        {
          response: response,
          entries: entries.size,
          waits: waits,
          current_pid: backoff.instance_variable_get(:@pid) == Process.pid,
          same_mutex: backoff.instance_variable_get(:@mutex).equal?(mutex),
        }
      end
      expect(result).to eq(response: "response", entries: 0, waits: [], current_pid: true, same_mutex: true)
      expect(owner).to be_alive
      expect(mutex).to be_locked
      expect(entries.values).to eq([entry])
      expect(entry.backoff).to eq(0.1)
      expect(backoff.instance_variable_get(:@pid)).to eq(Process.pid)
    ensure
      release << true
      owner.join
    end
  end

  it "discards inherited probes and waiters without changing the parent's active recovery" do
    fail_attempt
    entered, release, following = Queue.new, Queue.new, Queue.new
    probe = start_thread { attempt { entered << true; release.pop } }
    take(entered)
    waiter = start_thread { attempt { following << true } }
    entry = entries.values.first
    wait_until { entry.observers == 2 }
    begin
      result = in_child do
        response = attempt { "response" }
        { response: response, entries: entries.size, current_pid: backoff.instance_variable_get(:@pid) == Process.pid }
      end
      expect(result).to eq(response: "response", entries: 0, current_pid: true)
      expect(entry.probing).to eq(true)
      expect(entry.observers).to eq(2)
      expect(following).to be_empty
    ensure
      release << true
      probe.join
      waiter.join
    end
    expect(entries).to be_empty
  end

  it "does not let an inherited frame's cleanup delete new child failure history" do
    reader, writer = IO.pipe
    pid = nil
    child = false
    replacement = nil
    begin
      result = begin
        attempt do
          pid = fork
          if pid
            writer.close
          else
            child = true
            reader.close
            begin
              attempt { raise EOFError }
            rescue EOFError
            end
            replacement = entries.values.first
          end
        end
        if child
          {
            retained: entries.values.any? { |entry| entry.equal?(replacement) },
            backoff: replacement.backoff,
            observers: replacement.observers,
          }
        end
      rescue StandardError => error
        raise unless child
        { error: [error.class.name, error.message] }
      end
      if child
        writer.write(JSON.generate(result))
        writer.close
        exit! 0
      end
      result = Timeout.timeout(3) { JSON.parse(reader.read, symbolize_names: true) }
      Process.waitpid(pid)
      pid = nil
      expect(result).to eq(retained: true, backoff: 0.1, observers: 0)
      expect(entries).to be_empty
    ensure
      if pid
        Process.kill("KILL", pid)
        Process.waitpid(pid)
      end
      reader.close unless reader.closed?
      writer.close unless writer.closed?
    end
  end

  context "when disabled" do
    let(:backoff) { Kafka::ReconnectBackoff.new(0, 1) }

    it "bypasses socket inspection, clock, randomness, waiting, and fork checks" do
      expect(open_connection).not_to receive(:open?)
      expect(backoff).not_to receive(:monotonic_now)
      expect(backoff).not_to receive(:rand)
      expect(backoff).not_to receive(:sleep)
      expect(Process).not_to receive(:pid)
      3.times { fail_attempt(open_connection) }
      expect(attempt(open_connection) { :response }).to eq(:response)
    end
  end
end
