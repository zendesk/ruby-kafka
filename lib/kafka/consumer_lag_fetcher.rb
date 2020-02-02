module Kafka
    class ConsumerLagFetcher

        def initialize(cluster:, logger:, instrumenter:, group: )
            @cluster = cluster
            @logger = TaggedLogger.new(logger)
            @instrumenter = instrumenter
            @group = group

            @operation = ConsumerLagOperation.new(
                cluster: @cluster,
                logger: @logger,
                group: @group
            )
            @queue = Queue.new

            @running = false
        end

        def start
            return if @running

            @running = true

            @thread = Thread.new do
                while @running
                    puts 'thread'
                    loop
                    # sleep 1
                end
                @logger.info "#{@group} Fetcher thread exited."
            end
            @thread.abort_on_exception = true
        end

        def data?
            !@queue.empty?
        end

        def poll
            puts 'DEQUEUEING MESSAGE'
            group_lags = @queue.deq
            return group_lags
        end

        def stop
            return unless @running
            @running = false
            @thread.join
        end

        def handle_stop(*)
            @running = false
        end

        def running?
            @running
        end

        private

        def loop
            return unless @running
            messages = @operation.execute
            @queue << [messages]
            puts 'QUEUEING MESSAGE'
        end
    end
end