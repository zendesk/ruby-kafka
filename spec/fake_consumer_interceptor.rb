# frozen_string_literal: true

class FakeConsumerInterceptor
  def initialize(append_s: '', on_call_error: false)
    @append_s = append_s
    @on_call_error = on_call_error
  end

  def call(batch)
    if @on_call_error
      raise StandardError, "Something went wrong in the consumer interceptor"
    end
    intercepted_messages = batch.messages.collect do |message|
      Kafka::FetchedMessage.new(
        message: Kafka::Protocol::Message.new(
          value: message.value + @append_s,
          key: message.key,
          create_time: message.create_time,
          offset: message.offset
        ),
        topic: message.topic,
        partition: message.partition
      )
    end
    Kafka::FetchedBatch.new(
      topic: batch.topic,
      partition: batch.partition,
      highwater_mark_offset: batch.highwater_mark_offset,
      messages: intercepted_messages,
      last_offset: batch.last_offset,
      leader_epoch: batch.leader_epoch
    )
  end
end
