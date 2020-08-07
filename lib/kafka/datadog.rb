# frozen_string_literal: true

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

    class << self
      def statsd
        @statsd ||= ::Datadog::Statsd.new(host, port, namespace: namespace, tags: tags, socket_path: socket_path)
      end

      def statsd=(statsd)
        clear
        @statsd = statsd
      end

      def host
        @host
      end

      def host=(host)
        @host = host
        clear
      end

      def port
        @port
      end

      def port=(port)
        @port = port
        clear
      end

      def socket_path
        @socket_path
      end

      def socket_path=(socket_path)
        @socket_path = socket_path
        clear
      end

      def namespace
        @namespace ||= STATSD_NAMESPACE
      end

      def namespace=(namespace)
        @namespace = namespace
        clear
      end

      def tags
        @tags ||= []
      end

      def tags=(tags)
        @tags = tags
        clear
      end

      private

      def clear
        @statsd && @statsd.close
        @statsd = nil
      end
    end

    class StatsdSubscriber < ActiveSupport::Subscriber
      private

      %w[increment histogram count timing gauge].each do |type|
        define_method(type) do |*args, **kwargs|
          emit(type, *args, **kwargs)
        end
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

        timing("api.latency", event.duration, tags: tags)
        increment("api.calls", tags: tags)

        histogram("api.request_size", request_size, tags: tags)
        histogram("api.response_size", response_size, tags: tags)

        if event.payload.key?(:exception)
          increment("api.errors", tags: tags)
        end
      end

      attach_to "connection.kafka"
    end

    class ConsumerSubscriber < StatsdSubscriber
      def process_message(event)
        offset = event.payload.fetch(:offset)
        offset_lag = event.payload.fetch(:offset_lag)
        create_time = event.payload.fetch(:create_time)
        time_lag = create_time && ((Time.now - create_time) * 1000).to_i

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        if event.payload.key?(:exception)
          increment("consumer.process_message.errors", tags: tags)
        else
          timing("consumer.process_message.latency", event.duration, tags: tags)
          increment("consumer.messages", tags: tags)
        end

        gauge("consumer.offset", offset, tags: tags)
        gauge("consumer.lag", offset_lag, tags: tags)

        # Not all messages have timestamps.
        if time_lag
          gauge("consumer.time_lag", time_lag, tags: tags)
        end
      end

      def process_batch(event)
        offset = event.payload.fetch(:last_offset)
        messages = event.payload.fetch(:message_count)
        create_time = event.payload.fetch(:last_create_time)
        time_lag = create_time && ((Time.now - create_time) * 1000).to_i

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        if event.payload.key?(:exception)
          increment("consumer.process_batch.errors", tags: tags)
        else
          timing("consumer.process_batch.latency", event.duration, tags: tags)
          count("consumer.messages", messages, tags: tags)
        end

        gauge("consumer.offset", offset, tags: tags)

        if time_lag
          gauge("consumer.time_lag", time_lag, tags: tags)
        end
      end

      def fetch_batch(event)
        lag = event.payload.fetch(:offset_lag)
        batch_size = event.payload.fetch(:message_count)

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        histogram("consumer.batch_size", batch_size, tags: tags)
        gauge("consumer.lag", lag, tags: tags)
      end

      def join_group(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        timing("consumer.join_group", event.duration, tags: tags)

        if event.payload.key?(:exception)
          increment("consumer.join_group.errors", tags: tags)
        end
      end

      def sync_group(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        timing("consumer.sync_group", event.duration, tags: tags)

        if event.payload.key?(:exception)
          increment("consumer.sync_group.errors", tags: tags)
        end
      end

      def leave_group(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        timing("consumer.leave_group", event.duration, tags: tags)

        if event.payload.key?(:exception)
          increment("consumer.leave_group.errors", tags: tags)
        end
      end

      def loop(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        histogram("consumer.loop.duration", event.duration, tags: tags)
      end

      def pause_status(event)
        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition),
        }

        duration = event.payload.fetch(:duration)

        gauge("consumer.pause.duration", duration, tags: tags)
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
        buffer_fill_percentage = buffer_fill_ratio * 100.0

        tags = {
          client: client,
          topic: topic,
        }

        # This gets us the write rate.
        increment("producer.produce.messages", tags: tags.merge(topic: topic))

        # Information about typical/average/95p message size.
        histogram("producer.produce.message_size", message_size, tags: tags.merge(topic: topic))

        # Aggregate message size.
        count("producer.produce.message_size.sum", message_size, tags: tags.merge(topic: topic))

        # This gets us the avg/max buffer size per producer.
        histogram("producer.buffer.size", buffer_size, tags: tags)

        # This gets us the avg/max buffer fill ratio per producer.
        histogram("producer.buffer.fill_ratio", buffer_fill_ratio, tags: tags)
        histogram("producer.buffer.fill_percentage", buffer_fill_percentage, tags: tags)
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

        if event.payload.key?(:exception)
          increment("producer.deliver.errors", tags: tags)
        end

        timing("producer.deliver.latency", event.duration, tags: tags)

        # Messages delivered to Kafka:
        count("producer.deliver.messages", message_count, tags: tags)

        # Number of attempts to deliver messages:
        histogram("producer.deliver.attempts", attempts, tags: tags)
      end

      def ack_message(event)
        tags = {
          client: event.payload.fetch(:client_id),
          topic: event.payload.fetch(:topic),
        }

        # Number of messages ACK'd for the topic.
        increment("producer.ack.messages", tags: tags)

        # Histogram of delay between a message being produced and it being ACK'd.
        histogram("producer.ack.delay", event.payload.fetch(:delay), tags: tags)
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
          topic: topic,
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

      def drop_messages(event)
        tags = {
          client: event.payload.fetch(:client_id),
        }

        message_count = event.payload.fetch(:message_count)

        count("async_producer.dropped_messages", message_count, tags: tags)
      end

      attach_to "async_producer.kafka"
    end

    class FetcherSubscriber < StatsdSubscriber
      def loop(event)
        queue_size = event.payload.fetch(:queue_size)

        tags = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
        }

        gauge("fetcher.queue_size", queue_size, tags: tags)
      end

      attach_to "fetcher.kafka"
    end
  end
end
