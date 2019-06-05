# frozen_string_literal: true

#
#  Subscriber to ruby_kafka to report metrics to prometheus
#
#  Usage:
#     require "kafka/prometheus"
#
#  Once the file has been required, no further configuration is needed, all operational
#  metrics are automatically emitted (Unless PROMETHEUS_NO_AUTO_START is set).
#
#  By Peter Mustel, T2 Data AB
#
begin
  require 'prometheus/client'
rescue LoadError
  warn 'In order to report Kafka client metrics to Prometheus you need to install the `prometheus-client` gem.'
  raise
end

require 'active_support/subscriber'

module Kafka
  module Prometheus
    SIZE_BUCKETS = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000].freeze
    LATENCY_BUCKETS = [0.0001, 0.001, 0.01, 0.1, 1.0, 10, 100, 1000].freeze
    DELAY_BUCKETS = [1, 3, 10, 30, 100, 300, 1000, 3000, 10_000, 30_000].freeze

    class << self
      attr_accessor :registry

      def start(registry = ::Prometheus::Client.registry)
        @registry = registry
        ConnectionSubscriber.attach_to 'connection.kafka'
        ConsumerSubscriber.attach_to 'consumer.kafka'
        ProducerSubscriber.attach_to 'producer.kafka'
        AsyncProducerSubscriber.attach_to 'async_producer.kafka'
        FetcherSubscriber.attach_to 'fetcher.kafka'
      end
    end

    class ConnectionSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @api_calls = Prometheus.registry.counter(:api_calls, 'Total calls')
        @api_latency = Prometheus.registry.histogram(:api_latency, 'Latency', {}, LATENCY_BUCKETS)
        @api_request_size = Prometheus.registry.histogram(:api_request_size, 'Request size', {}, SIZE_BUCKETS)
        @api_response_size = Prometheus.registry.histogram(:api_response_size, 'Response size', {}, SIZE_BUCKETS)
        @api_errors = Prometheus.registry.counter(:api_errors, 'Errors')
      end

      def request(event)
        key = {
          client: event.payload.fetch(:client_id),
          api: event.payload.fetch(:api, 'unknown'),
          broker: event.payload.fetch(:broker_host)
        }
        request_size = event.payload.fetch(:request_size, 0)
        response_size = event.payload.fetch(:response_size, 0)

        @api_calls.increment(key)
        @api_latency.observe(key, event.duration)
        @api_request_size.observe(key, request_size)
        @api_response_size.observe(key, response_size)
        @api_errors.increment(key) if event.payload.key?(:exception)
      end
    end

    class ConsumerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @process_messages = Prometheus.registry.counter(:consumer_process_messages, 'Total messages')
        @process_message_errors = Prometheus.registry.counter(:consumer_process_message_errors, 'Total errors')
        @process_message_latency =
          Prometheus.registry.histogram(:consumer_process_message_latency, 'Latency', {}, LATENCY_BUCKETS)
        @offset_lag = Prometheus.registry.gauge(:consumer_offset_lag, 'Offset lag')
        @time_lag_now = Prometheus.registry.gauge(:consumer_time_lag_now, 'Time lag of message')
        @time_lag = Prometheus.registry.histogram(:consumer_time_lag, 'Time lag of message', {}, DELAY_BUCKETS)
        @process_batch_errors = Prometheus.registry.counter(:consumer_process_batch_errors, 'Total errors in batch')
        @process_batch_latency =
          Prometheus.registry.histogram(:consumer_process_batch_latency, 'Latency in batch', {}, LATENCY_BUCKETS)
        @batch_size = Prometheus.registry.histogram(:consumer_batch_size, 'Size of batch', {}, SIZE_BUCKETS)
        @join_group = Prometheus.registry.histogram(:consumer_join_group, 'Time to join group', {}, DELAY_BUCKETS)
        @join_group_errors = Prometheus.registry.counter(:consumer_join_group_errors, 'Total error in joining group')
        @sync_group = Prometheus.registry.histogram(:consumer_sync_group, 'Time to sync group', {}, DELAY_BUCKETS)
        @sync_group_errors = Prometheus.registry.counter(:consumer_sync_group_errors, 'Total error in syncing group')
        @leave_group = Prometheus.registry.histogram(:consumer_leave_group, 'Time to leave group', {}, DELAY_BUCKETS)
        @leave_group_errors = Prometheus.registry.counter(:consumer_leave_group_errors, 'Total error in leaving group')
        @pause_duration = Prometheus.registry.gauge(:consumer_pause_duration, 'Pause duration')
      end

      def process_message(event)
        key = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition)
        }

        offset_lag = event.payload.fetch(:offset_lag)
        create_time = event.payload.fetch(:create_time)

        time_lag = create_time && ((Time.now - create_time) * 1000).to_i

        if event.payload.key?(:exception)
          @process_message_errors.increment(key)
        else
          @process_message_latency.observe(key, event.duration)
          @process_messages.increment(key)
        end

        @offset_lag.set(key, offset_lag)

        # Not all messages have timestamps.
        return unless time_lag

        @time_lag_now.set(key, time_lag)
        @time_lag.observe(key, time_lag)
      end

      def process_batch(event)
        key = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition)
        }
        message_count = event.payload.fetch(:message_count)

        if event.payload.key?(:exception)
          @process_batch_errors.increment(key)
        else
          @process_batch_latency.observe(key, event.duration)
          @process_messages.increment(key, message_count)
        end
      end

      def fetch_batch(event)
        key = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition)
        }
        offset_lag = event.payload.fetch(:offset_lag)
        batch_size = event.payload.fetch(:message_count)

        @batch_size.observe(key, batch_size)
        @offset_lag.set(key, offset_lag)
      end

      def join_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @join_group.observe(key, event.duration)

        @join_group_errors.increment(key) if event.payload.key?(:exception)
      end

      def sync_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @sync_group.observe(key, event.duration)

        @sync_group_errors.increment(key) if event.payload.key?(:exception)
      end

      def leave_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @leave_group.observe(key, event.duration)

        @leave_group_errors.increment(key) if event.payload.key?(:exception)
      end

      def pause_status(event)
        key = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition)
        }

        duration = event.payload.fetch(:duration)
        @pause_duration.set(key, duration)
      end
    end

    class ProducerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @produce_messages = Prometheus.registry.counter(:producer_produced_messages, 'Produced messages total')
        @produce_message_size =
          Prometheus.registry.histogram(:producer_message_size, 'Message size', {}, SIZE_BUCKETS)
        @buffer_size = Prometheus.registry.histogram(:producer_buffer_size, 'Buffer size', {}, SIZE_BUCKETS)
        @buffer_fill_ratio = Prometheus.registry.histogram(:producer_buffer_fill_ratio, 'Buffer fill ratio')
        @buffer_fill_percentage = Prometheus.registry.histogram(:producer_buffer_fill_percentage, 'Buffer fill percentage')
        @produce_errors = Prometheus.registry.counter(:producer_produce_errors, 'Produce errors')
        @deliver_errors = Prometheus.registry.counter(:producer_deliver_errors, 'Deliver error')
        @deliver_latency =
          Prometheus.registry.histogram(:producer_deliver_latency, 'Delivery latency', {}, LATENCY_BUCKETS)
        @deliver_messages = Prometheus.registry.counter(:producer_deliver_messages, 'Total count of delivered messages')
        @deliver_attempts = Prometheus.registry.histogram(:producer_deliver_attempts, 'Delivery attempts')
        @ack_messages = Prometheus.registry.counter(:producer_ack_messages, 'Ack')
        @ack_delay = Prometheus.registry.histogram(:producer_ack_delay, 'Ack delay', {}, LATENCY_BUCKETS)
        @ack_errors = Prometheus.registry.counter(:producer_ack_errors, 'Ack errors')
      end

      def produce_message(event)
        client = event.payload.fetch(:client_id)
        key = { client: client, topic: event.payload.fetch(:topic) }

        message_size = event.payload.fetch(:message_size)
        buffer_size = event.payload.fetch(:buffer_size)
        max_buffer_size = event.payload.fetch(:max_buffer_size)
        buffer_fill_ratio = buffer_size.to_f / max_buffer_size.to_f
        buffer_fill_percentage = buffer_fill_ratio * 100.0

        # This gets us the write rate.
        @produce_messages.increment(key)
        @produce_message_size.observe(key, message_size)

        # This gets us the avg/max buffer size per producer.
        @buffer_size.observe({ client: client }, buffer_size)

        # This gets us the avg/max buffer fill ratio per producer.
        @buffer_fill_ratio.observe({ client: client }, buffer_fill_ratio)
        @buffer_fill_percentage.observe({ client: client }, buffer_fill_percentage)
      end

      def buffer_overflow(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }
        @produce_errors.increment(key)
      end

      def deliver_messages(event)
        key = { client: event.payload.fetch(:client_id) }
        message_count = event.payload.fetch(:delivered_message_count)
        attempts = event.payload.fetch(:attempts)

        @deliver_errors.increment(key) if event.payload.key?(:exception)
        @deliver_latency.observe(key, event.duration)

        # Messages delivered to Kafka:
        @deliver_messages.increment(key, message_count)

        # Number of attempts to deliver messages:
        @deliver_attempts.observe(key, attempts)
      end

      def ack_message(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        # Number of messages ACK'd for the topic.
        @ack_messages.increment(key)

        # Histogram of delay between a message being produced and it being ACK'd.
        @ack_delay.observe(key, event.payload.fetch(:delay))
      end

      def topic_error(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        @ack_errors.increment(key)
      end
    end

    class AsyncProducerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @queue_size = Prometheus.registry.histogram(:async_producer_queue_size, 'Queue size', {}, SIZE_BUCKETS)
        @queue_fill_ratio = Prometheus.registry.histogram(:async_producer_queue_fill_ratio, 'Queue fill ratio')
        @produce_errors = Prometheus.registry.counter(:async_producer_produce_errors, 'Producer errors')
        @dropped_messages = Prometheus.registry.counter(:async_producer_dropped_messages, 'Dropped messages')
      end

      def enqueue_message(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        queue_size = event.payload.fetch(:queue_size)
        max_queue_size = event.payload.fetch(:max_queue_size)
        queue_fill_ratio = queue_size.to_f / max_queue_size.to_f

        # This gets us the avg/max queue size per producer.
        @queue_size.observe(key, queue_size)

        # This gets us the avg/max queue fill ratio per producer.
        @queue_fill_ratio.observe(key, queue_fill_ratio)
      end

      def buffer_overflow(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }
        @produce_errors.increment(key)
      end

      def drop_messages(event)
        key = { client: event.payload.fetch(:client_id) }
        message_count = event.payload.fetch(:message_count)

        @dropped_messages.increment(key, message_count)
      end
    end

    class FetcherSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @queue_size = Prometheus.registry.gauge(:fetcher_queue_size, 'Queue size')
      end

      def loop(event)
        queue_size = event.payload.fetch(:queue_size)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        @queue_size.set({ client: client, group_id: group_id }, queue_size)
      end
    end
  end
end

# To enable testability, it is possible to skip the start until test time
Kafka::Prometheus.start unless defined?(PROMETHEUS_NO_AUTO_START)
