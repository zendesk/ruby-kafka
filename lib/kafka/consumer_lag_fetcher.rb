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
                    loop
                end
                @logger.info "#{@group} Fetcher thread exited."
            end
            @thread.abort_on_exception = true
        end

        def data?
            !@queue.empty?
        end

        def poll
            group_lags = @queue.deq
            puts 'polling'
            return group_lags
        end

        def stop
            return unless @running
            @thread.join
        end

        def handle_stop(*)
            @running = false
        end

        private

        def loop
            @logger.push_tags(@group.to_s)
            @instrumenter.instrument("loop.fetcher", {
                queue_size: @queue.size,
            })
            return unless @running
            messages = @operation.execute
            puts messages
            @queue << [messages]
        end
    end
end