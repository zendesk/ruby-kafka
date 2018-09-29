# frozen_string_literal: true

require "kafka/fetch_operation"
require "kafka/locked_queue"

module Kafka
  class Fetcher
    attr_reader :queue

    def initialize(cluster:, logger:, instrumenter:, max_queue_size:, group:, pause_manager:)
      @cluster = cluster
      @logger = logger
      @instrumenter = instrumenter
      @max_queue_size = max_queue_size
      @group = group
      @pause_manager = pause_manager

      @queue = LockedQueue.new
      @commands = Queue.new
      @next_offsets = Hash.new { |h, k| h[k] = {} }

      # Long poll until at least this many bytes can be fetched.
      @min_bytes = 1

      # Long poll at most this number of seconds.
      @max_wait_time = 1

      # The maximum number of bytes to fetch for any given fetch request.
      @max_bytes = 10485760

      # The maximum number of bytes to fetch per partition, by topic.
      @max_bytes_per_partition = {}

      @thread = Thread.new do
        loop while true
      end

      @thread.abort_on_exception = true
    end

    def subscribe(topic, max_bytes_per_partition:)
      @commands << [:subscribe, [topic, max_bytes_per_partition]]
    end

    def seek(topic, partition, offset)
      @commands << [:seek, [topic, partition, offset]]
    end

    def configure(min_bytes:, max_bytes:, max_wait_time:)
      @commands << [:configure, [min_bytes, max_bytes, max_wait_time]]
    end

    def start
      @commands << [:start, []]
    end

    def clean(topic, partition)
      @commands << [:clean, [topic, partition]]
    end

    def handle_start
      raise "already started" if @running

      @running = true
    end

    def stop
      @commands << [:stop, []]
    end

    def reset
      @commands << [:reset, []]
    end

    def data?
      !@queue.empty?
    end

    def poll
      @queue.deq
    end

    private

    def loop
      @instrumenter.instrument("loop.fetcher", {
        queue_size: @queue.size,
      })

      if !@commands.empty?
        cmd, args = @commands.deq

        @logger.debug "Handling fetcher command: #{cmd}"

        send("handle_#{cmd}", *args)
      elsif !@running
        sleep 0.1
      elsif @queue.size < @max_queue_size
        step
      else
        @logger.warn "Reached max fetcher queue size (#{@max_queue_size}), sleeping 1s"
        sleep 1
      end
    end

    def handle_configure(min_bytes, max_bytes, max_wait_time)
      @min_bytes = min_bytes
      @max_bytes = max_bytes
      @max_wait_time = max_wait_time
    end

    def handle_reset
      @next_offsets.clear
      @queue.clear
    end

    def handle_stop(*)
      @running = false

      # After stopping, we need to reconfigure the topics and partitions to fetch
      # from. Otherwise we'd keep fetching from a bunch of partitions we may no
      # longer be assigned.
      handle_reset
    end

    def handle_subscribe(topic, max_bytes_per_partition)
      @logger.info "Will fetch at most #{max_bytes_per_partition} bytes at a time per partition from #{topic}"
      @max_bytes_per_partition[topic] = max_bytes_per_partition
    end

    def handle_seek(topic, partition, offset)
      @instrumenter.instrument('seek.consumer',
                               group_id: @group.group_id,
                               topic: topic,
                               partition: partition,
                               offset: offset)
      @logger.info "Seeking #{topic}/#{partition} to offset #{offset}"
      @next_offsets[topic][partition] = offset
    end

    def handle_clean(topic, partition)
      @queue.map! do |queue|
        if queue[0] == :batches
          new_batches = queue[1].delete_if do |batch|
            batch.topic == topic && batch.partition == partition
          end
          [:batches, new_batches]
        else
          queue
        end
      end
    end

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

        @next_offsets[batch.topic][batch.partition] = batch.last_offset + 1 unless batch.unknown_last_offset?
      end

      @queue << [:batches, batches]
    rescue Kafka::NoPartitionsToFetchFrom
      @logger.warn "No partitions to fetch from, sleeping for 1s"
      sleep 1
    rescue Kafka::Error => e
      @queue << [:exception, e]
    end

    def fetch_batches
      @logger.debug "Fetching batches"

      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: @min_bytes,
        max_bytes: @max_bytes,
        max_wait_time: @max_wait_time,
      )

      @next_offsets.each do |topic, partitions|
        # Fetch at most this many bytes from any single partition.
        max_bytes = @max_bytes_per_partition[topic]

        partitions.each do |partition, offset|
          next if @pause_manager.paused?(topic, partition)
          operation.fetch_from_partition(topic, partition, offset: offset, max_bytes: max_bytes)
        end
      end

      operation.execute
    rescue UnknownTopicOrPartition
      @logger.error "Failed to fetch from some partitions. Maybe a rebalance has happened? Refreshing cluster info."

      # Our cluster information has become stale, we need to refresh it.
      @cluster.refresh_metadata!

      # Don't overwhelm the brokers in case this keeps happening.
      sleep 10

      retry
    rescue NoPartitionsToFetchFrom
      backoff = @max_wait_time > 0 ? @max_wait_time : 1

      @logger.info "There are no partitions to fetch from, sleeping for #{backoff}s"
      sleep backoff

      []
    end
  end
end
