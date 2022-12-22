# frozen_string_literal: true

module Kafka
  class ParallelHeartbeat < Heartbeat
    def initialize(group:, interval:, instrumenter:, logger:, timeout: nil, enabled: proc { false })
      super(group: group, interval: interval, instrumenter: instrumenter)

      @timeout = timeout
      @enable_proc = enabled
      @mutex = Mutex.new
      @thread = spawn_thread

      @logger = logger
    end

    def parallel
      @mutex.synchronize { @parallel_start = Time.now }
      yield
    ensure
      @mutex.synchronize { @parallel_start = nil }
    end

    def stop
      @mutex.synchronize { @thread.kill }
    end

    private

    def spawn_thread
      Thread.new do
        while true
          sleep_until_next_beat

          send_parallel_heartbeat
        end
      end
    end

    def sleep_until_next_beat
      now = Time.now
      sleep_until = @last_heartbeat + @interval + 1

      if sleep_until > now
        sleep(sleep_until - now)
      end
    end

    def send_parallel_heartbeat
      @mutex.synchronize do
        return unless @parallel_start

        duration = Time.now - @parallel_start
        if duration < @timeout && @enable_proc.call()
          begin
            @logger.info 'sending parallel heartbeat'
            trigger
          rescue => e
            @logger.error "heartbeat error: #{e.message}"
            sleep 5 # no sleep on next iteration if heartbeat was unsuccessfull
          end
        end
      end
    end
  end
end
