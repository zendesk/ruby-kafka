# frozen_string_literal: true

module Kafka
  class Heartbeat
    def initialize(group:, interval:)
      @group = group
      @interval = interval
      @last_heartbeat = Time.now
    end

    def trigger!
      @group.heartbeat
      @last_heartbeat = Time.now
    end

    def trigger
      trigger! if Time.now > @last_heartbeat + @interval
    end
  end
end
