module Kafka
  class Instrumenter
    NAMESPACE = "kafka"

    def initialize(default_payload = {})
      @default_payload = default_payload

      if defined?(ActiveSupport::Notifications)
        @backend = ActiveSupport::Notifications.instrumenter
      else
        @backend = nil
      end
    end

    def instrument(event_name, payload = {})
      if @backend
        payload.update(@default_payload)

        @backend.instrument("#{event_name}.#{NAMESPACE}", payload) { yield payload if block_given? }
      else
        yield payload if block_given?
      end
    end

    def start(event_name, payload = {})
      if @backend
        payload.update(@default_payload)

        @backend.start("#{event_name}.#{NAMESPACE}", payload)
      end
    end

    def finish(event_name, payload = {})
      if @backend
        payload.update(@default_payload)

        @backend.finish("#{event_name}.#{NAMESPACE}", payload)
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

    def start(event_name, payload = {})
      @backend.start(event_name, @extra_payload.merge(payload))
    end

    def finish(event_name, payload = {})
      @backend.finish(event_name, @extra_payload.merge(payload))
    end
  end
end
