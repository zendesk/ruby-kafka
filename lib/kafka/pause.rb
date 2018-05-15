# frozen_string_literal: true

module Kafka
  # Manages the pause state of a partition.
  #
  # The processing of messages in a partition can be paused, e.g. if there was
  # an exception during processing. This could be caused by a downstream service
  # not being available. A typical way of solving such an issue is to back off
  # for a little while and then try again. In order to do that, _pause_ the
  # partition.
  class Pause
    def initialize(clock: Time)
      @clock = clock
      @started_at = nil
      @pauses = 0
      @timeout = nil
      @max_timeout = nil
      @exponential_backoff = false
    end

    # Mark the partition as paused.
    #
    # If exponential backoff is enabled, each subsequent pause of a partition will
    # cause a doubling of the actual timeout, i.e. for pause number _n_, the actual
    # timeout will be _2^n * timeout_.
    #
    # Only when {#reset!} is called is this state cleared.
    #
    # @param timeout [nil, Integer] if specified, the partition will automatically
    #   resume after this many seconds.
    # @param exponential_backoff [Boolean] whether to enable exponential timeouts.
    def pause!(timeout: nil, max_timeout: nil, exponential_backoff: false)
      @started_at = @clock.now
      @timeout = timeout
      @max_timeout = max_timeout
      @exponential_backoff = exponential_backoff
      @pauses += 1
    end

    # Resumes the partition.
    #
    # The number of pauses is still retained, and if the partition is paused again
    # it may be with an exponential backoff.
    def resume!
      @started_at = nil
      @timeout = nil
      @max_timeout = nil
    end

    # Whether the partition is currently paused. The pause may have expired, in which
    # case {#expired?} should be checked as well.
    def paused?
      # This is nil if we're not currently paused.
      !@started_at.nil?
    end

    def pause_duration
      if paused?
        Time.now - @started_at
      else
        0
      end
    end

    # Whether the pause has expired.
    def expired?
      # We never expire the pause if timeout is nil.
      return false if @timeout.nil?

      # Have we passed the end of the pause duration?
      @clock.now >= ends_at
    end

    # Resets the pause state, ensuring that the next pause is not exponential.
    def reset!
      @pauses = 0
    end

    private

    def ends_at
      # Apply an exponential backoff to the timeout.
      backoff_factor = @exponential_backoff ? 2**(@pauses - 1) : 1
      timeout = backoff_factor * @timeout

      # If set, don't allow a timeout longer than max_timeout.
      timeout = @max_timeout if @max_timeout && timeout > @max_timeout

      @started_at + timeout
    end
  end
end
