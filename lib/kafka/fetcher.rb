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
      @commands = Queue.new
      @next_offsets = Hash.new { |h, k| h[k] = {} }

      @min_bytes = 1
      @max_bytes = 10485760
      @max_wait_time = 1

      @thread = Thread.new do
        loop while true
      end

      @thread.abort_on_exception = true
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

    def handle_start
      raise "already started" if @running

      @running = true
    end

    def stop
      @commands << [:stop, []]
    end

    private

    def loop
      if !@commands.empty?
        cmd, args = @commands.deq

        @logger.debug "Handling fetcher command: #{cmd}"

        send("handle_#{cmd}", *args)
      elsif !@running
        sleep 0.1
      elsif @queue.size < MAX_QUEUE_SIZE
        step
      else
        @logger.warn "Reached max fetcher queue size (#{MAX_QUEUE_SIZE}), sleeping 1s"
        sleep 1
      end
    end

    def handle_configure(min_bytes, max_bytes, max_wait_time)
      @min_bytes = min_bytes
      @max_bytes = max_bytes
      @max_wait_time = max_wait_time
    end

    def handle_stop(*)
      @running = false

      # After stopping, we need to reconfigure the topics and partitions to fetch
      # from. Otherwise we'd keep fetching from a bunch of partitions we may no
      # longer be assigned.
      @next_offsets.clear
    end

    def handle_seek(topic, partition, offset)
      @logger.info "Seeking #{topic}/#{partition} to offset #{offset}"
      @next_offsets[topic][partition] = offset
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

        @next_offsets[batch.topic][batch.partition] = batch.last_offset + 1
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
        partitions.each do |partition, offset|
          operation.fetch_from_partition(topic, partition, offset: offset)
        end
      end

      operation.execute
    rescue NoPartitionsToFetchFrom
      backoff = @max_wait_time > 0 ? @max_wait_time : 1

      @logger.info "There are no partitions to fetch from, sleeping for #{backoff}s"
      sleep backoff

      []
    end
  end
end
