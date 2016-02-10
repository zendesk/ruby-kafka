module Kafka
  class NullInstrumentation
    def self.instrument(name, payload = {})
      yield payload if block_given?
    end
  end

  if defined?(ActiveSupport::Notifications)
    Instrumentation = ActiveSupport::Notifications
  else
    Instrumentation = NullInstrumentation
  end
end
