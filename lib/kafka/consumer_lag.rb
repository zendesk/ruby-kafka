module Kafka
    class ConsumeGroupLag
        def initialize(cluster:, logger:, group_id:, fetcher:)
            @cluster = cluster
            @logger = logger
            @group_id = group_id
            @fetcher = fetcher

            @running = false
        end

        def fetch_lags
            lag_loop do
                lags = fetch_consumer_lags
                puts 'lags'
                yield lags
            end
        end

        def stop
            @running  = false
            @fetcher.stop
            @cluster.disconnnect
        end

        private

        def lag_loop
            @running = true
            @logger.push_tags(@group_id)
            
            @fetcher.start
            while running?
                begin
                    @instrumenter.instrument("loop.consumer_lag") do
                        yield
                    end
                rescue => exception
                    puts exception
                ensure
                    @fetcher.stop
                    @running = false
                    @logger.pop_tags
                end
            end
        end

        def fetch_consumer_lags
            if !@fecher.data?
                puts 'no messages'
                []
            else
                group_lags = @fetcher.poll
                puts 'poll'
                group_lags
            end
        end
    end
end