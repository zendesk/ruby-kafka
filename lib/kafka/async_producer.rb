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
    def initialize(sync_producer:, max_queue_size: 1000, delivery_threshold: 0, delivery_interval: 0, instrumenter:)
      raise ArgumentError unless max_queue_size > 0
      raise ArgumentError unless delivery_threshold >= 0
      raise ArgumentError unless delivery_interval >= 0

      @queue = Queue.new
      @max_queue_size = max_queue_size
      @instrumenter = instrumenter

      @worker = Worker.new(
        queue: @queue,
        producer: sync_producer,
        delivery_threshold: delivery_threshold,
      )

      # The timer will no-op if the delivery interval is zero.
      @timer = Timer.new(queue: @queue, interval: delivery_interval)
    end

    # Produces a message to the specified topic.
    #
    # @see Kafka::Producer#produce
    # @param (see Kafka::Producer#produce)
    # @raise [BufferOverflow] if the message queue is full.
    # @return [nil]
    def produce(value, topic:, **options)
      ensure_threads_running!

      buffer_overflow(topic) if @queue.size >= @max_queue_size

      args = [value, **options.merge(topic: topic)]
      @queue << [:produce, args]

      nil
    end

    # Asynchronously delivers the buffered messages. This method will return
    # immediately and the actual work will be done in the background.
    #
    # @see Kafka::Producer#deliver_messages
    # @return [nil]
    def deliver_messages
      @queue << [:deliver_messages, nil]

      nil
    end

    # Shuts down the producer, releasing the network resources used. This
    # method will block until the buffered messages have been delivered.
    #
    # @see Kafka::Producer#shutdown
    # @return [nil]
    def shutdown
      @queue << [:shutdown, nil]
      @worker_thread && @worker_thread.join

      nil
    end

    private

    def ensure_threads_running!
      @worker_thread = nil unless @worker_thread && @worker_thread.alive?
      @worker_thread ||= start_thread { @worker.run }

      @timer_thread = nil unless @timer_thread && @timer_thread.alive?
      @timer_thread ||= start_thread { @timer.run }
    end

    def start_thread(&block)
      thread = Thread.new(&block)
      thread.abort_on_exception = true
      thread
    end

    def buffer_overflow(topic)
      @instrumenter.instrument("buffer_overflow.producer", {
        topic: topic,
      })

      raise BufferOverflow
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
      def initialize(queue:, producer:, delivery_threshold:)
        @queue = queue
        @producer = producer
        @delivery_threshold = delivery_threshold
      end

      def run
        loop do
          operation, payload = @queue.pop

          case operation
          when :produce
            produce(*payload)
            deliver_messages if threshold_reached?
          when :deliver_messages
            deliver_messages
          when :shutdown
            # Deliver any pending messages first.
            deliver_messages

            # Stop the run loop.
            break
          else
            raise "Unknown operation #{operation.inspect}"
          end
        end
      ensure
        @producer.shutdown
      end

      private

      def produce(*args)
        @producer.produce(*args)
      rescue BufferOverflow
        deliver_messages
        retry
      end

      def deliver_messages
        @producer.deliver_messages
      rescue DeliveryFailed
        # Delivery failed.
      end

      def threshold_reached?
        @delivery_threshold > 0 &&
          @producer.buffer_size >= @delivery_threshold
      end
    end
  end
end
