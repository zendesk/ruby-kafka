# frozen_string_literal: true

module Kafka
  # Holds a list of interceptors that implement `call`
  # and wraps calls to a chain of custom interceptors.
  class Interceptors
    def initialize(interceptors:, logger:)
      @interceptors = interceptors || []
      @logger = TaggedLogger.new(logger)
    end

    # This method is called when the client produces a message or once the batches are fetched.
    # The message returned from the first call is passed to the second interceptor call, and so on in an
    # interceptor chain. This method does not throw exceptions.
    #
    # @param intercepted [Kafka::PendingMessage || Kafka::FetchedBatch] the produced message or
    #   fetched batch.
    #
    # @return [Kafka::PendingMessage || Kafka::FetchedBatch] the intercepted message or batch
    #   returned by the last interceptor.
    def call(intercepted)
      @interceptors.each do |interceptor|
        begin
          intercepted = interceptor.call(intercepted)
        rescue Exception => e
          @logger.warn "Error executing interceptor for topic: #{intercepted.topic} partition: #{intercepted.partition}: #{e.message}\n#{e.backtrace.join("\n")}"
        end
      end

      intercepted
    end
  end
end
