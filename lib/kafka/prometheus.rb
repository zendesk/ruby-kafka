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
        @api_calls = Prometheus.registry.counter(:api_calls, docstring: 'Total calls', labels: [:client, :api, :broker])
        @api_latency = Prometheus.registry.histogram(:api_latency, docstring: 'Latency', buckets: LATENCY_BUCKETS, labels: [:client, :api, :broker])
        @api_request_size = Prometheus.registry.histogram(:api_request_size, docstring: 'Request size', buckets: SIZE_BUCKETS, labels: [:client, :api, :broker])
        @api_response_size = Prometheus.registry.histogram(:api_response_size, docstring: 'Response size', buckets: SIZE_BUCKETS, labels: [:client, :api, :broker])
        @api_errors = Prometheus.registry.counter(:api_errors, docstring: 'Errors', labels: [:client, :api, :broker])
      end

      def request(event)
        key = {
          client: event.payload.fetch(:client_id),
          api: event.payload.fetch(:api, 'unknown'),
          broker: event.payload.fetch(:broker_host)
        }
        request_size = event.payload.fetch(:request_size, 0)
        response_size = event.payload.fetch(:response_size, 0)

        @api_calls.increment(labels: key)
        @api_latency.observe(event.duration, labels: key)
        @api_request_size.observe(request_size, labels: key)
        @api_response_size.observe(response_size, labels: key)
        @api_errors.increment(labels: key) if event.payload.key?(:exception)
      end
    end

    class ConsumerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @process_messages = Prometheus.registry.counter(:consumer_process_messages, docstring: 'Total messages', labels: [:client, :group_id, :topic, :partition])
        @process_message_errors = Prometheus.registry.counter(:consumer_process_message_errors, docstring: 'Total errors', labels: [:client, :group_id, :topic, :partition])
        @process_message_latency =
          Prometheus.registry.histogram(:consumer_process_message_latency, docstring: 'Latency', buckets: LATENCY_BUCKETS, labels: [:client, :group_id, :topic, :partition])
        @offset_lag = Prometheus.registry.gauge(:consumer_offset_lag, docstring: 'Offset lag', labels: [:client, :group_id, :topic, :partition])
        @time_lag = Prometheus.registry.gauge(:consumer_time_lag, docstring: 'Time lag of message', labels: [:client, :group_id, :topic, :partition])
        @process_batch_errors = Prometheus.registry.counter(:consumer_process_batch_errors, docstring: 'Total errors in batch', labels: [:client, :group_id, :topic, :partition])
        @process_batch_latency =
          Prometheus.registry.histogram(:consumer_process_batch_latency, docstring: 'Latency in batch', buckets: LATENCY_BUCKETS, labels: [:client, :group_id, :topic, :partition])
        @batch_size = Prometheus.registry.histogram(:consumer_batch_size, docstring: 'Size of batch', buckets: SIZE_BUCKETS, labels: [:client, :group_id, :topic, :partition])
        @join_group = Prometheus.registry.histogram(:consumer_join_group, docstring: 'Time to join group', buckets: DELAY_BUCKETS, labels: [:client, :group_id])
        @join_group_errors = Prometheus.registry.counter(:consumer_join_group_errors, docstring: 'Total error in joining group', labels: [:client, :group_id])
        @sync_group = Prometheus.registry.histogram(:consumer_sync_group, docstring: 'Time to sync group', buckets: DELAY_BUCKETS, labels: [:client, :group_id])
        @sync_group_errors = Prometheus.registry.counter(:consumer_sync_group_errors, docstring: 'Total error in syncing group', labels: [:client, :group_id])
        @leave_group = Prometheus.registry.histogram(:consumer_leave_group, docstring: 'Time to leave group', buckets: DELAY_BUCKETS, labels: [:client, :group_id])
        @leave_group_errors = Prometheus.registry.counter(:consumer_leave_group_errors, docstring: 'Total error in leaving group', labels: [:client, :group_id])
        @pause_duration = Prometheus.registry.gauge(:consumer_pause_duration, docstring: 'Pause duration', labels: [:client, :group_id, :topic, :partition])
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
          @process_message_errors.increment(labels: key)
        else
          @process_message_latency.observe(event.duration, labels: key)
          @process_messages.increment(labels: key)
        end

        @offset_lag.set(offset_lag, labels: key)

        # Not all messages have timestamps.
        return unless time_lag

        @time_lag.set(time_lag, labels: key)
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
          @process_batch_errors.increment(labels: key)
        else
          @process_batch_latency.observe(event.duration, labels: key)
          @process_messages.increment(by: message_count, labels: key)
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

        @batch_size.observe(batch_size, labels: key)
        @offset_lag.set(offset_lag, labels: key)
      end

      def join_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @join_group.observe(event.duration, labels: key)

        @join_group_errors.increment(labels: key) if event.payload.key?(:exception)
      end

      def sync_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @sync_group.observe(event.duration, labels: key)

        @sync_group_errors.increment(labels: key) if event.payload.key?(:exception)
      end

      def leave_group(event)
        key = { client: event.payload.fetch(:client_id), group_id: event.payload.fetch(:group_id) }
        @leave_group.observe(event.duration, labels: key)

        @leave_group_errors.increment(labels: key) if event.payload.key?(:exception)
      end

      def pause_status(event)
        key = {
          client: event.payload.fetch(:client_id),
          group_id: event.payload.fetch(:group_id),
          topic: event.payload.fetch(:topic),
          partition: event.payload.fetch(:partition)
        }

        duration = event.payload.fetch(:duration)
        @pause_duration.set(duration, labels: key)
      end
    end

    class ProducerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @produce_messages = Prometheus.registry.counter(:producer_produced_messages, docstring: 'Produced messages total', labels: [:client, :topic])
        @produce_message_size =
          Prometheus.registry.histogram(:producer_message_size, docstring: 'Message size', buckets: SIZE_BUCKETS, labels: [:client, :topic])
        @buffer_size = Prometheus.registry.histogram(:producer_buffer_size, docstring: 'Buffer size', buckets: SIZE_BUCKETS, labels: [:client])
        @buffer_fill_ratio = Prometheus.registry.histogram(:producer_buffer_fill_ratio, docstring: 'Buffer fill ratio', labels: [:client])
        @buffer_fill_percentage = Prometheus.registry.histogram(:producer_buffer_fill_percentage, docstring: 'Buffer fill percentage', labels: [:client])
        @produce_errors = Prometheus.registry.counter(:producer_produce_errors, docstring: 'Produce errors', labels: [:client, :topic])
        @deliver_errors = Prometheus.registry.counter(:producer_deliver_errors, docstring: 'Deliver error', labels: [:client])
        @deliver_latency =
          Prometheus.registry.histogram(:producer_deliver_latency, docstring: 'Delivery latency', buckets: LATENCY_BUCKETS, labels: [:client])
        @deliver_messages = Prometheus.registry.counter(:producer_deliver_messages, docstring: 'Total count of delivered messages', labels: [:client])
        @deliver_attempts = Prometheus.registry.histogram(:producer_deliver_attempts, docstring: 'Delivery attempts', labels: [:client])
        @ack_messages = Prometheus.registry.counter(:producer_ack_messages, docstring: 'Ack', labels: [:client, :topic])
        @ack_delay = Prometheus.registry.histogram(:producer_ack_delay, docstring: 'Ack delay', buckets: LATENCY_BUCKETS, labels: [:client, :topic])
        @ack_errors = Prometheus.registry.counter(:producer_ack_errors, docstring: 'Ack errors', labels: [:client, :topic])
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
        @produce_messages.increment(labels: key)
        @produce_message_size.observe(message_size, labels: key)

        # This gets us the avg/max buffer size per producer.
        @buffer_size.observe(buffer_size, labels: { client: client })

        # This gets us the avg/max buffer fill ratio per producer.
        @buffer_fill_ratio.observe(buffer_fill_ratio, labels: { client: client })
        @buffer_fill_percentage.observe(buffer_fill_percentage, labels: { client: client })
      end

      def buffer_overflow(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }
        @produce_errors.increment(labels: key)
      end

      def deliver_messages(event)
        key = { client: event.payload.fetch(:client_id) }
        message_count = event.payload.fetch(:delivered_message_count)
        attempts = event.payload.fetch(:attempts)

        @deliver_errors.increment(labels: key) if event.payload.key?(:exception)
        @deliver_latency.observe(event.duration, labels: key)

        # Messages delivered to Kafka:
        @deliver_messages.increment(by: message_count, labels: key)

        # Number of attempts to deliver messages:
        @deliver_attempts.observe(attempts, labels: key)
      end

      def ack_message(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        # Number of messages ACK'd for the topic.
        @ack_messages.increment(labels: key)

        # Histogram of delay between a message being produced and it being ACK'd.
        @ack_delay.observe(event.payload.fetch(:delay), labels: key)
      end

      def topic_error(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        @ack_errors.increment(labels: key)
      end
    end

    class AsyncProducerSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @queue_size = Prometheus.registry.histogram(:async_producer_queue_size, docstring: 'Queue size', buckets: SIZE_BUCKETS, labels: [:client, :topic])
        @queue_fill_ratio = Prometheus.registry.histogram(:async_producer_queue_fill_ratio, docstring: 'Queue fill ratio', labels: [:client, :topic])
        @produce_errors = Prometheus.registry.counter(:async_producer_produce_errors, docstring: 'Producer errors', labels: [:client, :topic])
        @dropped_messages = Prometheus.registry.counter(:async_producer_dropped_messages, docstring: 'Dropped messages', labels: [:client])
      end

      def enqueue_message(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }

        queue_size = event.payload.fetch(:queue_size)
        max_queue_size = event.payload.fetch(:max_queue_size)
        queue_fill_ratio = queue_size.to_f / max_queue_size.to_f

        # This gets us the avg/max queue size per producer.
        @queue_size.observe(queue_size, labels: key)

        # This gets us the avg/max queue fill ratio per producer.
        @queue_fill_ratio.observe(queue_fill_ratio, labels: key)
      end

      def buffer_overflow(event)
        key = { client: event.payload.fetch(:client_id), topic: event.payload.fetch(:topic) }
        @produce_errors.increment(labels: key)
      end

      def drop_messages(event)
        key = { client: event.payload.fetch(:client_id) }
        message_count = event.payload.fetch(:message_count)
        @dropped_messages.increment(by: message_count, labels: key)
      end
    end

    class FetcherSubscriber < ActiveSupport::Subscriber
      def initialize
        super
        @queue_size = Prometheus.registry.gauge(:fetcher_queue_size, docstring: 'Queue size', labels: [:client, :group_id])
      end

      def loop(event)
        queue_size = event.payload.fetch(:queue_size)
        client = event.payload.fetch(:client_id)
        group_id = event.payload.fetch(:group_id)

        @queue_size.set(queue_size, labels: { client: client, group_id: group_id })
      end
    end
  end
end

# To enable testability, it is possible to skip the start until test time
Kafka::Prometheus.start unless defined?(PROMETHEUS_NO_AUTO_START)
