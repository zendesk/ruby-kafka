# frozen_string_literal: true

begin
  require "statsd"
rescue LoadError
  $stderr.puts "In order to report Kafka client metrics to Statsd you need to install the `statsd-ruby` gem."
  raise
end

require "active_support/subscriber"

module Kafka
  # Reports operational metrics to a Statsd agent.
  #
  #     require "kafka/statsd"
  #
  #     # Default is "ruby_kafka".
  #     Kafka::Statsd.namespace = "custom-namespace"
  #
  #     # Default is "127.0.0.1".
  #     Kafka::Statsd.host = "statsd.something.com"
  #
  #     # Default is 8125.
  #     Kafka::Statsd.port = 1234
  #
  # Once the file has been required, no further configuration is needed – all operational
  # metrics are automatically emitted.
  module Statsd
    DEFAULT_NAMESPACE = "ruby_kafka"
    DEFAULT_HOST = '127.0.0.1'
    DEFAULT_PORT = 8125

    def self.statsd
      @statsd ||= ::Statsd.new(DEFAULT_HOST, DEFAULT_PORT).tap { |sd| sd.namespace = DEFAULT_NAMESPACE }
    end

    def self.host=(host)
      statsd.host = host
      statsd.connect if statsd.respond_to?(:connect)
    end

    def self.port=(port)
      statsd.port = port
      statsd.connect if statsd.respond_to?(:connect)
    end

    def self.namespace=(namespace)
      statsd.namespace = namespace
    end

    class StatsdSubscriber < ActiveSupport::Subscriber
      private

      %w[increment count timing gauge].each do |type|
        define_method(type) do |*args|
          Kafka::Statsd.statsd.send(type, *args)
        end
      end
    end

    class ConnectionSubscriber < StatsdSubscriber
      def request(event)
        client = event.payload.fetch(:client_id)
        api = event.payload.fetch(:api, "unknown")
        request_size = event.payload.fetch(:request_size, 0)
        response_size = event.payload.fetch(:response_size, 0)
        broker = event.payload.fetch(:broker_host)

        timing("api.#{client}.#{api}.#{broker}.latency", event.duration)
        increment("api.#{client}.#{api}.#{broker}.calls")

        timing("api.#{client}.#{api}.#{broker}.request_size", request_size)
        timing("api.#{client}.#{api}.#{broker}.response_size", response_size)

        if event.payload.key?(:exception)
          increment("api.#{client}.#{api}.#{broker}.errors")
        end
      end

      attach_to "connection.kafka"
    end

    class ConsumerSubscriber < StatsdSubscriber
      def process_message(event)
        offset_lag = event.payload.fetch(:offset_lag)
        create_time = event.payload.fetch(:create_time)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)
        topic = event.payload.fetch(:topic)
        partition = event.payload.fetch(:partition)

        time_lag = create_time && ((Time.now - create_time) * 1000).to_i

        if event.payload.key?(:exception)
          increment("consumer.#{client}.#{group_id}.#{topic}.#{partition}.process_message.errors")
        else
          timing("consumer.#{client}.#{group_id}.#{topic}.#{partition}.process_message.latency", event.duration)
          increment("consumer.#{client}.#{group_id}.#{topic}.#{partition}.messages")
        end

        gauge("consumer.#{client}.#{group_id}.#{topic}.#{partition}.lag", offset_lag)

        # Not all messages have timestamps.
        if time_lag
          gauge("consumer.#{client}.#{group_id}.#{topic}.#{partition}.time_lag", time_lag)
        end
      end

      def process_batch(event)
        messages = event.payload.fetch(:message_count)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)
        topic = event.payload.fetch(:topic)
        partition = event.payload.fetch(:partition)

        if event.payload.key?(:exception)
          increment("consumer.#{client}.#{group_id}.#{topic}.#{partition}.process_batch.errors")
        else
          timing("consumer.#{client}.#{group_id}.#{topic}.#{partition}.process_batch.latency", event.duration)
          count("consumer.#{client}.#{group_id}.#{topic}.#{partition}.messages", messages)
        end
      end

      def fetch_batch(event)
        lag = event.payload.fetch(:offset_lag)
        batch_size = event.payload.fetch(:message_count)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)
        topic = event.payload.fetch(:topic)
        partition = event.payload.fetch(:partition)

        count("consumer.#{client}.#{group_id}.#{topic}.#{partition}.batch_size", batch_size)
        gauge("consumer.#{client}.#{group_id}.#{topic}.#{partition}.lag", lag)
      end

      def join_group(event)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        timing("consumer.#{client}.#{group_id}.join_group", event.duration)

        if event.payload.key?(:exception)
          increment("consumer.#{client}.#{group_id}.join_group.errors")
        end
      end

      def sync_group(event)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        timing("consumer.#{client}.#{group_id}.sync_group", event.duration)

        if event.payload.key?(:exception)
          increment("consumer.#{client}.#{group_id}.sync_group.errors")
        end
      end

      def leave_group(event)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        timing("consumer.#{client}.#{group_id}.leave_group", event.duration)

        if event.payload.key?(:exception)
          increment("consumer.#{client}.#{group_id}.leave_group.errors")
        end
      end

      def pause_status(event)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)
        topic = event.payload.fetch(:topic)
        partition = event.payload.fetch(:partition)

        duration = event.payload.fetch(:duration)

        gauge("consumer.#{client}.#{group_id}.#{topic}.#{partition}.pause.duration", duration)
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

        # This gets us the write rate.
        increment("producer.#{client}.#{topic}.produce.messages")

        timing("producer.#{client}.#{topic}.produce.message_size", message_size)

        # This gets us the avg/max buffer size per producer.
        timing("producer.#{client}.buffer.size", buffer_size)

        # This gets us the avg/max buffer fill ratio per producer.
        timing("producer.#{client}.buffer.fill_ratio", buffer_fill_ratio)
        timing("producer.#{client}.buffer.fill_percentage", buffer_fill_percentage)
      end

      def buffer_overflow(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)

        increment("producer.#{client}.#{topic}.produce.errors")
      end

      def deliver_messages(event)
        client = event.payload.fetch(:client_id)
        message_count = event.payload.fetch(:delivered_message_count)
        attempts = event.payload.fetch(:attempts)

        if event.payload.key?(:exception)
          increment("producer.#{client}.deliver.errors")
        end

        timing("producer.#{client}.deliver.latency", event.duration)

        # Messages delivered to Kafka:
        count("producer.#{client}.deliver.messages", message_count)

        # Number of attempts to deliver messages:
        timing("producer.#{client}.deliver.attempts", attempts)
      end

      def ack_message(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)

        # Number of messages ACK'd for the topic.
        increment("producer.#{client}.#{topic}.ack.messages")

        # Histogram of delay between a message being produced and it being ACK'd.
        timing("producer.#{client}.#{topic}.ack.delay", event.payload.fetch(:delay))
      end

      def topic_error(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)

        increment("producer.#{client}.#{topic}.ack.errors")
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

        # This gets us the avg/max queue size per producer.
        timing("async_producer.#{client}.#{topic}.queue.size", queue_size)

        # This gets us the avg/max queue fill ratio per producer.
        timing("async_producer.#{client}.#{topic}.queue.fill_ratio", queue_fill_ratio)
      end

      def buffer_overflow(event)
        client = event.payload.fetch(:client_id)
        topic = event.payload.fetch(:topic)

        increment("async_producer.#{client}.#{topic}.produce.errors")
      end

      def drop_messages(event)
        client = event.payload.fetch(:client_id)
        message_count = event.payload.fetch(:message_count)

        count("async_producer.#{client}.dropped_messages", message_count)
      end

      attach_to "async_producer.kafka"
    end

    class FetcherSubscriber < StatsdSubscriber
      def loop(event)
        queue_size = event.payload.fetch(:queue_size)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        gauge("fetcher.#{client}.#{group_id}.queue_size", queue_size)
      end

      attach_to "fetcher.kafka"
    end
  end
end
