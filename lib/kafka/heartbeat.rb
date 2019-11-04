# frozen_string_literal: true

module Kafka
  class Heartbeat
    def initialize(group:, interval:, instrumenter:)
      @group = group
      @interval = interval
      @last_heartbeat = Time.now
      @instrumenter = instrumenter
    end

    def trigger!
      @instrumenter.instrument('heartbeat.consumer',
                               group_id: @group.group_id,
                               topic_partitions: @group.assigned_partitions) do
        @group.heartbeat
        @last_heartbeat = Time.now
      end
    end

    def trigger
      trigger! if Time.now > @last_heartbeat + @interval
    end
  end
end
