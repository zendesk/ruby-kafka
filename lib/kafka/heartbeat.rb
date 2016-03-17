module Kafka
  class Heartbeat
    def initialize(group:, interval:)
      @group = group
      @interval = interval
      @last_heartbeat = Time.now
    end

    def send_if_necessary
      if Time.now > @last_heartbeat + @interval
        @group.heartbeat
        @last_heartbeat = Time.now
      end
    end
  end
end
