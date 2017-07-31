require "kafka/fetcher"

module Kafka
  class FetchController
    def initialize(cluster:, group:, logger:)
      @cluster = cluster
      @group = group
      @logger = logger
      @output = Queue.new
      @fetchers = []
      @threads = []
    end

    def fetch_batches
      @output.deq
    end

    def shutdown
      @fetchers.each do |_, commands|
        commands << [:shutdown, {}]
      end

      @threads.each do |thread|
        Thread.join(thread)
      end
    end

    def setup
      topics_by_broker = {}

      @group.subscribed_partitions.each do |topic, partitions|
        partitions.each do |partition, options|
          broker = @cluster.get_leader(topic, partition)

          topics_by_broker[broker] ||= {}
          topics_by_broker[broker][topic] ||= []
          topics_by_broker[broker][topic] << partition
        end
      end

      @fetchers = topics_by_broker.map {|broker, topic_and_partitions|
        commands = Queue.new

        fetcher = Fetcher.new(
          broker: broker,
          assignments: topic_and_partitions,
          commands: commands,
          output: @output,
          logger: logger,
        )

        [fetcher, commands]
      }

      @threads = @fetchers.map {|fetcher, _|
        Thread.new { fetcher.run }.tap {|thread|
          thread.abort_on_exception = true
        }
      }
    end
  end
end
