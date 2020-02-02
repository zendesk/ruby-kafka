module Kafka
    class ConsumeGroupLag
        def initialize(cluster:, logger:, group_id:, fetcher:)
            @cluster = cluster
            @logger = logger
            @group_id = group_id
            @fetcher = fetcher

            @running = false
        end

        def fetch_lags(&block)
            start do
                consumer_lags = fetch_consumer_lags
                if !consumer_lags.empty?
                    consumer_lags.each(&block)
                end
            end
        end
        
        def running?
            @running
        end

        def stop
            @running = false
            @fetcher.stop
            @cluster.disconnnect
        end

        private

        def start
            @running = true
            
            @fetcher.start

            while running?
                yield
            end
        ensure
            @fetcher.stop
            @running = false
        end

        def fetch_consumer_lags
            if !@fetcher.data?
                # TODO: remove this print
                print '.'
                []
            else
                group_lags = @fetcher.poll
                group_lags
            end
        end
    end
end