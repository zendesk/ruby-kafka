# frozen_string_literal: true

require "thread"

module Kafka
  class ReconnectBackoff
    Entry = Struct.new(:backoff, :retry_at, :generation, :probing, :observers, :last_used, :condition)

    def initialize(backoff, max_backoff)
      @backoff = seconds(backoff, "reconnect_backoff")
      @max_backoff = seconds(max_backoff, "reconnect_backoff_max")
      if @max_backoff < @backoff
        raise ArgumentError, "reconnect_backoff_max must be at least reconnect_backoff"
      end
      return if @backoff.zero?

      @pid = Process.pid
      @mutex = Mutex.new
      @entries = {}
      @retention = [300, @max_backoff].max
      @next_prune = 0
    end

    def with_attempt(host, port, connection)
      return yield if @backoff.zero?

      key = [host.to_s.dup.freeze, port.to_s.dup.freeze].freeze
      cold = !(connection && connection.open?)

      Thread.handle_interrupt(Object => :never) do
        entry = checkout(key)
        probing = false
        begin
          probing, generation = claim_probe(entry) if cold
          generation = Thread.handle_interrupt(Object => :immediate) { wait_until_ready(entry) } if probing

          begin
            result = Thread.handle_interrupt(Object => :immediate) { yield }
          rescue StandardError => error
            record_failure(entry) if transport_error?(error)
            raise
          else
            synchronize do
              if cold && entry.generation == generation
                entry.backoff = 0.0
                entry.retry_at = 0.0
              end
            end
            result
          end
        ensure
          synchronize do
            entry.probing = false if probing
            entry.observers -= 1
            entry.last_used = monotonic_now
            if entry.observers.zero? && entry.backoff.zero? && @entries[key].equal?(entry)
              @entries.delete(key)
            end
            entry.condition.broadcast if probing
          end
        end
      end
    end

    private

    def seconds(value, name)
      unless (value.is_a?(Integer) || value.is_a?(Float)) && value.to_f.finite? && value >= 0
        raise ArgumentError, "#{name} must be finite, non-negative seconds (Integer or Float)"
      end
      value.to_f
    end

    def synchronize
      @mutex.synchronize do
        pid = Process.pid
        if pid != @pid
          @entries = {}
          @next_prune = 0
          @pid = pid
        end
        yield
      end
    end

    def checkout(key)
      synchronize do
        now = monotonic_now
        if now >= @next_prune
          @entries.delete_if { |_, entry| entry.observers.zero? && now - entry.last_used >= @retention }
          @next_prune = now + 60
        end
        entry = @entries[key] ||= Entry.new(0.0, 0.0, 0, false, 0, now, ConditionVariable.new)
        entry.observers += 1
        entry
      end
    end

    def claim_probe(entry)
      synchronize do
        while entry.probing && !entry.backoff.zero?
          Thread.handle_interrupt(Object => :immediate) { entry.condition.wait(@mutex) }
        end
        claimed = !entry.backoff.zero?
        entry.probing = true if claimed
        [claimed, entry.generation]
      end
    end

    def wait_until_ready(entry)
      loop do
        delay, generation = synchronize { [entry.retry_at - monotonic_now, entry.generation] }
        return generation if delay <= 0
        sleep delay
      end
    end

    def record_failure(entry)
      jitter = 0.8 + rand * 0.4
      synchronize do
        entry.backoff = entry.backoff.zero? ? @backoff : [entry.backoff * 2, @max_backoff].min
        entry.retry_at = monotonic_now + [entry.backoff * jitter, @max_backoff].min
        entry.generation += 1
      end
    end

    def transport_error?(error)
      while error
        return false if error.is_a?(ProtocolError) || error.is_a?(IdleConnection)
        return true if error.is_a?(ConnectionError) || error.is_a?(SystemCallError) ||
            error.is_a?(IOError) || error.is_a?(SocketError)
        error = error.cause
      end
      false
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
