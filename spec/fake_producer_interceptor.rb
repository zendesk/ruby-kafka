# frozen_string_literal: true

class FakeProducerInterceptor
  def initialize(append_s: '', on_call_error: false)
    @append_s = append_s
    @on_call_error = on_call_error
  end

  def call(pending_message)
    if @on_call_error
      raise StandardError, "Something went wrong in the producer interceptor"
    end
    Kafka::PendingMessage.new(
      value: pending_message.value + @append_s,
      key: pending_message.key,
      headers: pending_message.headers,
      topic: pending_message.topic,
      partition: pending_message.partition,
      partition_key: pending_message.partition_key,
      create_time: pending_message.create_time
    )
  end
end
