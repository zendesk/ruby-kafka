module Kafka
  class Instrumenter
    def initialize(client_id:)
      @client_id = client_id

      if defined?(ActiveSupport::Notifications)
        @backend = ActiveSupport::Notifications
      else
        @backend = nil
      end
    end

    def instrument(event_name, payload = {}, &block)
      if @backend
        @backend.instrument(event_name, payload, &block)
      else
        yield payload
      end
    end
  end
end
