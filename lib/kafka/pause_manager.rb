# frozen_string_literal: true

require "kafka/pause"

module Kafka
  class PauseManager
    def initialize
      @pauses = Hash.new {|h, k|
        h[k] = Hash.new {|h2, k2|
          h2[k2] = Pause.new
        }
      }
    end

    def pause!(topic, partition, timeout:, max_timeout:, exponential_backoff:)
      pause_for(topic, partition).pause!(
        timeout: timeout,
        max_timeout: max_timeout,
        exponential_backoff: exponential_backoff,
      )
    end

    def pause?(topic, partition)
      pause = pause_for(topic, partition)
      pause.paused? && !pause.expired?
    end

    def resume!(topic, partition)
      pause_for(topic, partition).resume!
    end

    def reset!(topic, partition)
      pause_for(topic, partition).reset!
    end

    private

    def pause_for(topic, partition)
      @pauses[topic][partition]
    end
  end
end
