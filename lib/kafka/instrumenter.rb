# frozen_string_literal: true

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
        block.call(payload) if block
      end
    end
  end

  class DecoratingInstrumenter
    def initialize(backend, extra_payload = {})
      @backend = backend
      @extra_payload = extra_payload
    end

    def instrument(event_name, payload = {}, &block)
      @backend.instrument(event_name, @extra_payload.merge(payload), &block)
    end
  end
end
