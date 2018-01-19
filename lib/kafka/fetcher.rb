require "kafka/fetch_operation"

module Kafka
  class Fetcher
    MAX_QUEUE_SIZE = 100

    attr_reader :queue

    def initialize(cluster:, logger:, instrumenter:)
      @cluster = cluster
      @logger = logger
      @instrumenter = instrumenter

      @queue = Queue.new
      @next_offsets = Hash.new { |h, k| h[k] = {} }
      @thread = nil
    end

    def seek(topic, partition, offset)
      @next_offsets[topic][partition] = offset
    end

    def start(min_bytes:, max_bytes:, max_wait_time:)
      @min_bytes = min_bytes
      @max_bytes = max_bytes
      @max_wait_time = max_wait_time

      raise "already started" if @running

      @running = true

      @thread = Thread.new do
        while @running
          if @queue.size < MAX_QUEUE_SIZE
            step
          else
            @logger.warn "Reached max fetcher queue size (#{MAX_QUEUE_SIZE}), sleeping 1s"
            sleep 1
          end
        end
      end

      @thread.abort_on_exception = true
    end

    def stop
      @running = false
    end

    private

    def step
      batches = fetch_batches

      batches.each do |batch|
        unless batch.empty?
          @instrumenter.instrument("fetch_batch.consumer", {
            topic: batch.topic,
            partition: batch.partition,
            offset_lag: batch.offset_lag,
            highwater_mark_offset: batch.highwater_mark_offset,
            message_count: batch.messages.count,
          })
        end

        @next_offsets[batch.topic][batch.partition] = batch.last_offset + 1
      end

      @queue << [:batches, batches]
    rescue Kafka::Error => e
      @queue << [:exception, e]
    end

    def fetch_batches
      @logger.info "=== FETCHING BATCHES ==="

      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: @min_bytes,
        max_bytes: @max_bytes,
        max_wait_time: @max_wait_time,
      )

      @next_offsets.each do |topic, partitions|
        partitions.each do |partition, offset|
          operation.fetch_from_partition(topic, partition, offset: offset)
        end
      end

      operation.execute
    rescue NoPartitionsToFetchFrom
      backoff = @max_wait_time > 0 ? @max_wait_time : 1

      @logger.info "There are no partitions to fetch from, sleeping for #{backoff}s"
      sleep backoff

      retry
    end
  end
end
