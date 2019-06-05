# frozen_string_literal: true

PROMETHEUS_NO_AUTO_START = true
require "kafka/prometheus"

describe Kafka::Prometheus do
  let(:exception) { false }

  before do
    # Must make sure we start with empty registry for each context
    @registry = ::Prometheus::Client::Registry.new
    Kafka::Prometheus.start(@registry)

    instrumenter = if exception
                     Kafka::Instrumenter.new(client_id: 'test', exception: exception)
                   else
                     Kafka::Instrumenter.new(client_id: 'test')
                   end
    instrumenter.instrument(hook, payload)
  end

  context 'when requesting a connection' do
    let(:key) { { client: 'test', api: 'foo', broker: 'somehost' } }
    let(:payload) { { broker_host: 'somehost', api: 'foo', request_size: 101, response_size: 4000 } }
    let(:hook) { 'request.connection' }

    it 'emits metrics to the api_calls' do
      metric = @registry.get(:api_calls)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end

    it 'emits metrics to api_latency' do
      metric = @registry.get(:api_latency)
      expect(metric).not_to be_nil
    end

    it 'emits metrics to api_request_size' do
      metric = @registry.get(:api_request_size)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 10=>0.0, 100=>0.0, 1000=>1.0, 10000=>1.0, 100000=>1.0, 1000000=>1.0})
    end

    it 'emits metrics to api_response_size' do
      metric = @registry.get(:api_response_size)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 10=>0.0, 100=>0.0, 1000=>0.0, 10000=>1.0, 100000=>1.0, 1000000=>1.0})
    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to api_errors' do
        metric = @registry.get(:api_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer is processing a message' do
    let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
    let(:payload) do
      {
        group_id: 'group1',
        topic: 'AAA',
        partition: 4,
        offset: 1,
        offset_lag: 500,
        create_time: Time.now - 5
      }
    end
    let(:hook) { 'process_message.consumer' }

    it 'emits metrics to consumer_offset_lag' do
      metric = @registry.get(:consumer_offset_lag)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 500
    end

    it 'emits metrics to consumer_process_messages' do
      metric = @registry.get(:consumer_process_messages)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end

    it 'emits metrics to consumer_process_message_latency' do
      metric = @registry.get(:consumer_process_message_latency)
      expect(metric).not_to be_nil
    end

    it 'emits metrics to consumer_time_lag_now' do
      metric = @registry.get(:consumer_time_lag_now)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to be_within(1).of(5000)
    end

    it 'emits metrics to consumer_time_lag' do
      metric = @registry.get(:consumer_time_lag)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 3=>0.0, 10=>0.0, 30=>0.0, 100=>0.0, 300=>0.0,
                                     1000=>0.0, 3000=>0.0, 10000=>1.0, 30000=>1.0})
    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to consumer_process_message_errors' do
        metric = @registry.get(:consumer_process_message_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer is processing a batch' do
    let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
    let(:payload) do
      {
        group_id: 'group1',
        topic: 'AAA',
        partition: 4,
        last_offset: 100,
        message_count: 7
      }
    end
    let(:hook) { 'process_batch.consumer' }

    it 'emits metrics consumer_process_messages' do
      metric = @registry.get(:consumer_process_messages)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 7
    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to consumer_process_batch_errors' do
        metric = @registry.get(:consumer_process_batch_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer is fetching a batch' do
    let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
    let(:payload) do
      {
        group_id: 'group1',
        topic: 'AAA',
        partition: 4,
        offset_lag: 7,
        message_count: 123
      }
    end
    let(:hook) { 'fetch_batch.consumer' }

    it 'emits metrics consumer_offset_lag' do
      metric = @registry.get(:consumer_offset_lag)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 7
    end

    it 'emits metrics consumer_batch_size' do
      metric = @registry.get(:consumer_batch_size)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 10=>0.0, 100=>0.0, 1000=>1.0, 10000=>1.0, 100000=>1.0, 1000000=>1.0})
    end
  end

  context 'when a consumer is joining a group' do
    let(:key) { { client: 'test', group_id: 'group1' } }
    let(:payload) { { group_id: 'group1' } }
    let(:hook) { 'join_group.consumer' }

    it 'emits metrics consumer_join_group' do
      metric = @registry.get(:consumer_join_group)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>1.0, 3=>1.0, 10=>1.0, 30=>1.0, 100=>1.0,
                                     300=>1.0, 1000=>1.0, 3000=>1.0, 10000=>1.0, 30000=>1.0})
    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to consumer_join_group_errors' do
        metric = @registry.get(:consumer_join_group_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer is syncing a group' do
    let(:key) { { client: 'test', group_id: 'group1' } }
    let(:payload) { { group_id: 'group1' } }
    let(:hook) { 'sync_group.consumer' }

    it 'emits metrics consumer_sync_group' do
      metric = @registry.get(:consumer_sync_group)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>1.0, 3=>1.0, 10=>1.0, 30=>1.0, 100=>1.0,
                                     300=>1.0, 1000=>1.0, 3000=>1.0, 10000=>1.0, 30000=>1.0})
    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to consumer_sync_group_errors' do
        metric = @registry.get(:consumer_sync_group_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer is leaving a group' do
    let(:key) { { client: 'test', group_id: 'group1' } }
    let(:payload) { { group_id: 'group1' } }
    let(:hook) { 'leave_group.consumer' }

    it 'emits metrics consumer_leave_group' do
      metric = @registry.get(:consumer_leave_group)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>1.0, 3=>1.0, 10=>1.0, 30=>1.0, 100=>1.0,
                                     300=>1.0, 1000=>1.0, 3000=>1.0, 10000=>1.0, 30000=>1.0})

    end

    context 'with expection' do
      let(:exception) { true }

      it 'emits metrics to consumer_leave_group_errors' do
        metric = @registry.get(:consumer_leave_group_errors)
        expect(metric).not_to be_nil
        expect(metric.get(key)).to eq 1
      end
    end
  end

  context 'when a consumer pauses status' do
    let(:key) { { client: 'test', group_id: 'group1', topic: 'AAA', partition: 4 } }
    let(:payload) { { group_id: 'group1', topic: 'AAA', partition: 4,  duration: 111 } }
    let(:hook) { 'pause_status.consumer' }

    it 'emits metrics to consumer_pause_duration' do
      metric = @registry.get(:consumer_pause_duration)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 111
    end
  end

  context 'when a producer produces a message' do
    let(:key) { { client: 'test', topic: 'AAA' } }
    let(:payload) do
      {
        group_id: 'group1',
        topic: 'AAA',
        partition: 4,
        buffer_size: 1000,
        max_buffer_size: 10000,
        message_size: 123
      }
    end
    let(:hook) { 'produce_message.producer' }

    it 'emits metrics producer_produced_messages' do
      metric = @registry.get(:producer_produced_messages)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end

    it 'emits metric producer_message_size' do
      metric = @registry.get(:producer_message_size)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 10=>0.0, 100=>0.0, 1000=>1.0, 10000=>1.0, 100000=>1.0, 1000000=>1.0})
    end

    it 'emits metric buffer_fill_ratio' do
      metric = @registry.get(:producer_buffer_fill_ratio)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({0.005=>0.0, 0.01=>0.0, 0.025=>0.0, 0.05=>0.0, 0.1=>0.0,
                                     0.25=>0.0, 0.5=>0.0, 1=>0.0, 2.5=>0.0, 5=>0.0, 10=>0.0})
    end
  end

  context 'when a producer gets topic error' do
    let(:key) { { client: 'test', topic: 'AAA' } }
    let(:payload) { { group_id: 'group1', topic: 'AAA' } }
    let(:hook) { 'topic_error.producer' }

    it 'emits metrics ack_error' do
      metric = @registry.get(:producer_ack_errors)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end
  end

  context 'when a producer gets buffer overflow' do
    let(:key) { { client: 'test', topic: 'AAA' } }
    let(:payload) { { topic: 'AAA' } }
    let(:hook) { 'buffer_overflow.producer' }

    it 'emits metrics producer_produce_errors' do
      metric = @registry.get(:producer_produce_errors)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end
  end

  context 'when a producer deliver_messages' do
    let(:key) { { client: 'test' } }
    let(:payload) { { delivered_message_count: 123,  attempts: 2 } }
    let(:hook) { 'deliver_messages.producer' }

    it 'emits metrics producer_deliver_messages' do
      metric = @registry.get(:producer_deliver_messages)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 123
    end

    it 'emits metrics producer_deliver_attempts' do
      metric = @registry.get(:producer_deliver_attempts)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({0.005=>0.0, 0.01=>0.0, 0.025=>0.0, 0.05=>0.0, 0.1=>0.0,
                                     0.25=>0.0, 0.5=>0.0, 1=>0.0, 2.5=>1.0, 5=>1.0, 10=>1.0})
    end
  end

  context 'when a asynch producer enqueues a message' do
    let(:key) { { client: 'test', topic: 'AAA' } }
    let(:payload) { { group_id: 'group1', topic: 'AAA' } }
    let(:hook) { 'topic_error.async_producer' }

    it 'emits metrics async_producer_queue_size' do
      metric = @registry.get(:async_producer_queue_size)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({1=>0.0, 10=>0.0, 100=>0.0, 1000=>0.0, 10000=>0.0, 100000=>0.0, 1000000=>0.0})
    end

    it 'emits metrics async_producer_queue fill_ratio' do
      metric = @registry.get(:async_producer_queue_fill_ratio)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq({0.005=>0.0, 0.01=>0.0, 0.025=>0.0, 0.05=>0.0, 0.1=>0.0,
                                     0.25=>0.0, 0.5=>0.0, 1=>0.0, 2.5=>0.0, 5=>0.0, 10=>0.0})
    end
  end

  context 'when a asynch producer gets buffer overflow' do
    let(:key) { { client: 'test', topic: 'AAA' } }
    let(:payload) { { topic: 'AAA' } }
    let(:hook) { 'buffer_overflow.async_producer' }

    it 'emits metrics async_producer_produce_errors' do
      metric = @registry.get(:async_producer_produce_errors)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 1
    end
  end

  context 'when a asynch producer gets dropped messages' do
    let(:key) { { client: 'test' } }
    let(:payload) { { message_count: 4 } }
    let(:hook) { 'drop_messages.async_producer' }

    it 'emits metrics async_producer_dropped_messages' do
      metric = @registry.get(:async_producer_dropped_messages)
      expect(metric).not_to be_nil
      expect(metric.get(key)).to eq 4
    end
  end
end
