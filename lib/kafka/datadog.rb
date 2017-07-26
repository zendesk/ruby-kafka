begin
  require "datadog/statsd"
rescue LoadError
  $stderr.puts "In order to report Kafka client metrics to Datadog you need to install the `dogstatsd-ruby` gem."
  raise
end

require "active_support/subscriber"

module Kafka

  # Reports operational metrics to a Datadog agent using the modified Statsd protocol.
  #
  #     require "kafka/datadog"
  #
  #     # Default is "ruby_kafka".
  #     Kafka::Datadog.namespace = "custom-namespace"
  #
  #     # Default is "127.0.0.1".
  #     Kafka::Datadog.host = "statsd.something.com"
  #
  #     # Default is 8125.
  #     Kafka::Datadog.port = 1234
  #
  # Once the file has been required, no further configuration is needed – all operational
  # metrics are automatically emitted.
  module Datadog
    STATSD_NAMESPACE = "ruby_kafka"

    def self.statsd
      @statsd ||= ::Datadog::Statsd.new(::Datadog::Statsd::DEFAULT_HOST, ::Datadog::Statsd::DEFAULT_PORT, namespace: STATSD_NAMESPACE)
    end

    def self.host=(host)
      statsd.host = host
    end

    def self.port=(port)
      statsd.port = port
    end

    def self.namespace=(namespace)
      statsd.namespace = namespace
    end

    def self.tags=(tags)
      statsd.tags = tags
    end

    class StatsdSubscriber < ActiveSupport::Subscriber
      private

      %w[increment histogram count timing gauge].each do |type|
        define_method(type) do |*args|
          emit(type, *args)
        end
      end

      def batch(&block)
        Kafka::Datadog.statsd.batch(&block)
      end

      def emit(type, *args, tags: {})
        tags = tags.map {|k, v| "#{k}:#{v}" }.to_a

        Kafka::Datadog.statsd.send(type, *args, tags: tags)
      end
    end

    class ConnectionSubscriber < StatsdSubscriber
      def request(event)
        client = event.payload.fetch(:client_id)
        api = event.payload.fetch(:api, "unknown")
        request_size = event.payload.fetch(:request_size, 0)
        response_size = event.payload.fetch(:response_size, 0)
        broker = event.payload.fetch(:broker_host)

        tags = {
          client: client,
          api: api,
          broker: broker
        }

        batch do
          timing("api.latency", event.duration, tags: tags)
          increment("api.calls", tags: tags)

          histogram("api.request_size", request_size, tags: tags)
          histogram("api.response_size", response_size, tags: tags)

          if event.payload.key?(:exception)
            increment("api.errors", tags: tags)
          end
        end
      end

      attach_to "connection.kafka"
    end

    class ConsumerSubscriber < StatsdSubscriber
      def process_message(event)
        lag = event.payload.fetch(:offset_lag)

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        batch do
          if event.payload.key?(:exception)
            increment("consumer.process_message.errors", tags: tags)
          else
            timing("consumer.process_message.latency", event.duration, tags: tags)
            increment("consumer.messages", tags: tags)
          end

          gauge("consumer.lag", lag, tags: tags)
        end
      end

      def process_batch(event)
        messages = event.payload.fetch(:message_count)

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        batch do
          if event.payload.key?(:exception)
            increment("consumer.process_batch.errors", tags: tags)
          else
            timing("consumer.process_batch.latency", event.duration, tags: tags)
            count("consumer.messages", messages, tags: tags)
          end
        end
      end

      attach_to "consumer.kafka"
    end

    class ProducerSubscriber < StatsdSubscriber
      def produce_message(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)
        message_size = event.payload.fetch(:message_size)
        buffer_size = event.payload.fetch(:buffer_size)
        max_buffer_size = event.payload.fetch(:max_buffer_size)
        buffer_fill_ratio = buffer_size.to_f / max_buffer_size.to_f

        tags = {
          client: client,
        }

        batch do
          # This gets us the write rate.
          increment("producer.produce.messages", tags: tags.merge(topic: topic))

          histogram("producer.produce.message_size", message_size, tags: tags.merge(topic: topic))

          # This gets us the avg/max buffer size per producer.
          histogram("producer.buffer.size", buffer_size, tags: tags)

          # This gets us the avg/max buffer fill ratio per producer.
          histogram("producer.buffer.fill_ratio", buffer_fill_ratio, tags: tags)
        end
      end

      def buffer_overflow(event)
        tags = {
          client: event.payload.fetch(:client_id),
          topic: event.payload.fetch(:topic),
        }

        increment("producer.produce.errors", tags: tags)
      end

      def deliver_messages(event)
        client = event.payload.fetch(:client_id)
        message_count = event.payload.fetch(:delivered_message_count)
        attempts = event.payload.fetch(:attempts)

        tags = {
          client: client,
        }

        batch do
          if event.payload.key?(:exception)
            increment("producer.deliver.errors", tags: tags)
          end

          timing("producer.deliver.latency", event.duration, tags: tags)

          # Messages delivered to Kafka:
          count("producer.deliver.messages", message_count, tags: tags)

          # Number of attempts to deliver messages:
          histogram("producer.deliver.attempts", attempts, tags: tags)
        end
      end

      def ack_message(event)
        tags = {
          client: event.payload.fetch(:client_id),
          topic: event.payload.fetch(:topic),
        }

        batch do
          # Number of messages ACK'd for the topic.
          increment("producer.ack.messages", tags: tags)

          # Histogram of delay between a message being produced and it being ACK'd.
          histogram("producer.ack.delay", event.payload.fetch(:delay), tags: tags)
        end
      end

      def topic_error(event)
        tags = {
          client: event.payload.fetch(:client_id),
          topic: event.payload.fetch(:topic)
        }

        increment("producer.ack.errors", tags: tags)
      end

      attach_to "producer.kafka"
    end

    class AsyncProducerSubscriber < StatsdSubscriber
      def enqueue_message(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)
        queue_size = event.payload.fetch(:queue_size)
        max_queue_size = event.payload.fetch(:max_queue_size)
        queue_fill_ratio = queue_size.to_f / max_queue_size.to_f

        tags = {
          client: client,
        }

        # This gets us the avg/max queue size per producer.
        histogram("async_producer.queue.size", queue_size, tags: tags)

        # This gets us the avg/max queue fill ratio per producer.
        histogram("async_producer.queue.fill_ratio", queue_fill_ratio, tags: tags)
      end

      def buffer_overflow(event)
        tags = {
          client: event.payload.fetch(:client_id),
          topic: event.payload.fetch(:topic),
        }

        increment("async_producer.produce.errors", tags: tags)
      end

      attach_to "async_producer.kafka"
    end
  end
end
