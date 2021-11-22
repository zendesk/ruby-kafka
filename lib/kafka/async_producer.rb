# frozen_string_literal: true

require "thread"

module Kafka

  # A Kafka producer that does all its work in the background so as to not block
  # the calling thread. Calls to {#deliver_messages} are asynchronous and return
  # immediately.
  #
  # In addition to this property it's possible to define automatic delivery
  # policies. These allow placing an upper bound on the number of buffered
  # messages and the time between message deliveries.
  #
  # * If `delivery_threshold` is set to a value _n_ higher than zero, the producer
  #   will automatically deliver its messages once its buffer size reaches _n_.
  # * If `delivery_interval` is set to a value _n_ higher than zero, the producer
  #   will automatically deliver its messages every _n_ seconds.
  #
  # By default, automatic delivery is disabled and you'll have to call
  # {#deliver_messages} manually.
  #
  # ## Buffer Overflow and Backpressure
  #
  # The calling thread communicates with the background thread doing the actual
  # work using a thread safe queue. While the background thread is busy delivering
  # messages, new messages will be buffered in the queue. In order to avoid
  # the queue growing uncontrollably in cases where the background thread gets
  # stuck or can't follow the pace of the calling thread, there's a maximum
  # number of messages that is allowed to be buffered. You can configure this
  # value by setting `max_queue_size`.
  #
  # If you produce messages faster than the background producer thread can
  # deliver them to Kafka you will eventually fill the producer's buffer. Once
  # this happens, the background thread will stop popping messages off the
  # queue until it can successfully deliver the buffered messages. The queue
  # will therefore grow in size, potentially hitting the `max_queue_size` limit.
  # Once this happens, calls to {#produce} will raise a {BufferOverflow} error.
  #
  # Depending on your use case you may want to slow down the rate of messages
  # being produced or perhaps halt your application completely until the
  # producer can deliver the buffered messages and clear the message queue.
  #
  # ## Example
  #
  #     producer = kafka.async_producer(
  #       # Keep at most 1.000 messages in the buffer before delivering:
  #       delivery_threshold: 1000,
  #
  #       # Deliver messages every 30 seconds:
  #       delivery_interval: 30,
  #     )
  #
  #     # There's no need to manually call #deliver_messages, it will happen
  #     # automatically in the background.
  #     producer.produce("hello", topic: "greetings")
  #
  #     # Remember to shut down the producer when you're done with it.
  #     producer.shutdown
  #
  class AsyncProducer
    # Initializes a new AsyncProducer.
    #
    # @param sync_producer [Kafka::Producer] the synchronous producer that should
    #   be used in the background.
    # @param max_queue_size [Integer] the maximum number of messages allowed in
    #   the queue.
    # @param delivery_threshold [Integer] if greater than zero, the number of
    #   buffered messages that will automatically trigger a delivery.
    # @param delivery_interval [Integer] if greater than zero, the number of
    #   seconds between automatic message deliveries.
    #
    def initialize(sync_producer:, max_queue_size: 1000, delivery_threshold: 0, delivery_interval: 0, max_retries: -1, retry_backoff: 0, instrumenter:, logger:)
      raise ArgumentError unless max_queue_size > 0
      raise ArgumentError unless delivery_threshold >= 0
      raise ArgumentError unless delivery_interval >= 0

      @queue = Queue.new
      @max_queue_size = max_queue_size
      @instrumenter = instrumenter
      @logger = TaggedLogger.new(logger)

      @worker = Worker.new(
        queue: @queue,
        producer: sync_producer,
        delivery_threshold: delivery_threshold,
        max_retries: max_retries,
        retry_backoff: retry_backoff,
        instrumenter: instrumenter,
        logger: logger
      )

      # The timer will no-op if the delivery interval is zero.
      @timer = Timer.new(queue: @queue, interval: delivery_interval)

      @thread_mutex = Mutex.new
    end

    # Produces a message to the specified topic.
    #
    # @see Kafka::Producer#produce
    # @param (see Kafka::Producer#produce)
    # @raise [BufferOverflow] if the message queue is full.
    # @return [nil]
    def produce(value, topic:, **options)
      # We want to fail fast if `topic` isn't a String
      topic = topic.to_str

      ensure_threads_running!

      if @queue.size >= @max_queue_size
        buffer_overflow topic,
          "Cannot produce to #{topic}, max queue size (#{@max_queue_size} messages) reached"
      end

      args = [value, **options.merge(topic: topic)]
      @queue << [:produce, args]

      @instrumenter.instrument("enqueue_message.async_producer", {
        topic: topic,
        queue_size: @queue.size,
        max_queue_size: @max_queue_size,
      })

      nil
    end

    # Asynchronously delivers the buffered messages. This method will return
    # immediately and the actual work will be done in the background.
    #
    # @see Kafka::Producer#deliver_messages
    # @return [nil]
    def deliver_messages
      ensure_threads_running!

      @queue << [:deliver_messages, nil]

      nil
    end

    # Shuts down the producer, releasing the network resources used. This
    # method will block until the buffered messages have been delivered.
    #
    # @see Kafka::Producer#shutdown
    # @return [nil]
    def shutdown
      ensure_threads_running!

      @timer_thread && @timer_thread.exit
      @queue << [:shutdown, nil]
      @worker_thread && @worker_thread.join

      nil
    end

    private

    def ensure_threads_running!
      return if worker_thread_alive? && timer_thread_alive?

      @thread_mutex.synchronize do
        @worker_thread = Thread.new { @worker.run } unless worker_thread_alive?
        @timer_thread = Thread.new { @timer.run } unless timer_thread_alive?
      end
    end

    def worker_thread_alive?
      !!@worker_thread && @worker_thread.alive?
    end

    def timer_thread_alive?
      !!@timer_thread && @timer_thread.alive?
    end

    def buffer_overflow(topic, message)
      @instrumenter.instrument("buffer_overflow.async_producer", {
        topic: topic,
      })

      raise BufferOverflow, message
    end

    class Timer
      def initialize(interval:, queue:)
        @queue = queue
        @interval = interval
      end

      def run
        # Permanently sleep if the timer interval is zero.
        Thread.stop if @interval.zero?

        loop do
          sleep(@interval)
          @queue << [:deliver_messages, nil]
        end
      end
    end

    class Worker
      def initialize(queue:, producer:, delivery_threshold:, max_retries: -1, retry_backoff: 0, instrumenter:, logger:)
        @queue = queue
        @producer = producer
        @delivery_threshold = delivery_threshold
        @max_retries = max_retries
        @retry_backoff = retry_backoff
        @instrumenter = instrumenter
        @logger = TaggedLogger.new(logger)
      end

      def run
        @logger.push_tags(@producer.to_s)
        @logger.info "Starting async producer in the background..."

        do_loop
      rescue Exception => e
        @logger.error "Unexpected Kafka error #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
        @logger.error "Async producer crashed!"
      ensure
        @producer.shutdown
        @logger.pop_tags
      end

      private

      def do_loop
        loop do
          begin
            operation, payload = @queue.pop

            case operation
            when :produce
              produce(payload[0], **payload[1])
              deliver_messages if threshold_reached?
            when :deliver_messages
              deliver_messages
            when :shutdown
              begin
                # Deliver any pending messages first.
                @producer.deliver_messages
              rescue Error => e
                @logger.error("Failed to deliver messages during shutdown: #{e.message}")

                @instrumenter.instrument("drop_messages.async_producer", {
                  message_count: @producer.buffer_size + @queue.size,
                })
              end

              # Stop the run loop.
              break
            else
              raise "Unknown operation #{operation.inspect}"
            end
          end
        end
      rescue Kafka::Error => e
        @logger.error "Unexpected Kafka error #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
        @logger.info "Restarting in 10 seconds..."

        sleep 10
        retry
      end

      def produce(value, **kwargs)
        retries = 0
        begin
          @producer.produce(value, **kwargs)
        rescue BufferOverflow => e
          deliver_messages
          if @max_retries == -1
            retry
          elsif retries < @max_retries
            retries += 1
            sleep @retry_backoff**retries
            retry
          else
            @logger.error("Failed to asynchronously produce messages due to BufferOverflow")
            @instrumenter.instrument("error.async_producer", { error: e })
          end
        end
      end

      def deliver_messages
        @producer.deliver_messages
      rescue DeliveryFailed, ConnectionError => e
        # Failed to deliver messages -- nothing to do but log and try again later.
        @logger.error("Failed to asynchronously deliver messages: #{e.message}")
        @instrumenter.instrument("error.async_producer", { error: e })
      end

      def threshold_reached?
        @delivery_threshold > 0 &&
          @producer.buffer_size >= @delivery_threshold
      end
    end
  end
end
