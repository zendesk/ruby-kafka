# frozen_string_literal: true

require "fake_consumer_interceptor"
require "fake_producer_interceptor"

describe "Interceptors", functional: true do
  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "intercept produced and consumed message" do
    producer_interceptor = FakeProducerInterceptor.new(append_s: 'producer')
    producer = kafka.producer(max_retries: 3, retry_backoff: 1, interceptors: [producer_interceptor])
    producer.produce("hello", topic: topic)
    producer.deliver_messages
    producer.shutdown

    consumer_interceptor = FakeConsumerInterceptor.new(append_s: 'consumer')
    consumer = kafka.consumer(group_id: SecureRandom.uuid, fetcher_max_queue_size: 1, interceptors: [consumer_interceptor])
    consumer.subscribe(topic)
    consumer.each_message do |message|
      expect(message.value).to eq "helloproducerconsumer"
      break
    end
    consumer.stop
  end
end
