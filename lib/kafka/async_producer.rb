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
  # The calling thread communicates with the background thread doing the actual
  # work using a thread safe queue. While the background thread is busy delivering
  # messages, new messages will be buffered in the queue. In order to avoid
  # the queue growing uncontrollably in cases where the background thread gets
  # stuck or can't follow the pace of the calling thread, there's a maximum
  # number of messages that is allowed to be buffered. You can configure this
  # value by setting `max_queue_size`.
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
    def initialize(sync_producer:, max_queue_size: 1000, delivery_threshold: 0, delivery_interval: 0)
      raise ArgumentError unless max_queue_size > 0
      raise ArgumentError unless delivery_threshold >= 0
      raise ArgumentError unless delivery_interval >= 0

      @queue = Queue.new
      @max_queue_size = max_queue_size

      @worker_thread = Thread.new do
        worker = Worker.new(
          queue: @queue,
          producer: sync_producer,
          delivery_threshold: delivery_threshold,
        )

        worker.run
      end

      @worker_thread.abort_on_exception = true

      if delivery_interval > 0
        Thread.new do
          Timer.new(queue: @queue, interval: delivery_interval).run
        end
      end
    end

    # Produces a message to the specified topic.
    #
    # @see Kafka::Producer#produce
    # @param (see Kafka::Producer#produce)
    # @raise [BufferOverflow] if the message queue is full.
    # @return [nil]
    def produce(*args)
      raise BufferOverflow if @queue.size >= @max_queue_size
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
      @worker_thread.join

      nil
    end

    class Timer
      def initialize(interval:, queue:)
        @queue = queue
        @interval = interval
      end

      def run
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
            @producer.produce(*payload)
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

      def deliver_messages
        @producer.deliver_messages
      rescue FailedToSendMessages
        # Delivery failed.
      end

      def threshold_reached?
        @delivery_threshold > 0 &&
          @producer.buffer_size >= @delivery_threshold
      end
    end
  end
end
