module Kafka
  class Instrumenter
    NAMESPACE = "kafka"

    def initialize(default_payload = {})
      @default_payload = default_payload

      if defined?(ActiveSupport::Notifications)
        @backend = ActiveSupport::Notifications
      else
        @backend = nil
      end
    end

    def instrument(event_name, payload = {}, &block)
      if @backend
        payload.update(@default_payload)

        @backend.instrument("#{event_name}.#{NAMESPACE}", payload, &block)
      else
        yield payload
      end
    end
  end
end
