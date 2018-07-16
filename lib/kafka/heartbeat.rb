# frozen_string_literal: true

module Kafka
  # :nodoc:all:
  class Heartbeat

    LOOP_WAIT_TIME = 1 # seconds

    # :nodoc:
    def initialize(group:, consumer:, interval:, logger:, poll_timeout:Integer::MAX,
                   instrumenter:)
      @group = group
      @consumer = consumer
      @interval = interval
      @logger = logger
      @poll_timeout = poll_timeout
      @instrumenter = instrumenter
      @signal_to_stop = false
      @started = false
    end

    def trigger!
      @group.heartbeat
      @last_heartbeat = Time.now
    end

    def trigger
      trigger! if Time.now > @last_heartbeat + @interval
    end

    # :nodoc:
    def start
      return if @started
      @started = true
      Thread.new do
        @logger.info("Starting heartbeat thread for #{@group}")
        reset_timestamps
        start_heartbeat_loop
      end
    end

    # :nodoc:
    def message_started
      @last_message_time = Time.now
    end

    # :nodoc:
    def stop
      @signal_to_stop = true
    end

    # :nodoc:
    def reset_timestamps
      @last_heartbeat = Time.now
      @last_message_time = Time.now
    end

    private

    def start_heartbeat_loop
      loop do
        sleep(LOOP_WAIT_TIME)
        if @signal_to_stop
          @started = false
          break
        end
        send_heartbeat_if_needed
      end
    end

    def send_heartbeat_if_needed
      return if Time.now - @last_heartbeat < @interval
      return unless @group.member?
      if Time.now - @last_message_time > @poll_timeout
        @logger.error("Message exceeded timeout value of #{@poll_timeout} for #{@group}")
        @group.leave
        return
      end
      begin
        @logger.info("Sending heartbeat for #{@group}")
        @instrumenter.instrument('heartbeat.consumer',
                                 group_id: @group.group_id,
                                 topic_partitions: @group.assigned_partitions) do
                                   self.trigger!
                                 end
      rescue StandardError => e

        @logger.error("Got error sending heartbeat for #{@group}: #{e.message}")
        @consumer.join_group
      end
    end

  end
end
