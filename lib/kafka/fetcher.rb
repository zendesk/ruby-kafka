module Kafka

  # A _fetcher_ is responsible for connecting to a single Kafka broker and
  # fetching messages from it based on instructions from a Consumer.
  #
  # A Fetcher instance runs in a separate thread and communicates over a
  # thread-safe queue.
  class Fetcher
    def initialize(broker:, commands:, output:, logger:)
      @broker = broker
      @commands = commands
      @output = output
      @logger = logger

      @min_bytes = 1
      @max_wait_time = 30

      @running = true
      @assignments = Hash.new
    end

    def run
      while @running
        if @commands.empty?
          fetch
        else
          command, params = @commands.deq
          public_send(command, **params)
        end
      end
    end

    def fetch(**)
      topics = {}
      
      @assignments.each do |topic, partitions|
        topics[topic] = {}

        max_bytes = 1048576

        partitions.each do |partition|
          offset = -2

          topics[topic][partition] = {
            fetch_offset: offset,
            max_bytes: max_bytes,
          }
        end
      end

      response = @broker.fetch_messages(
        min_bytes: @min_bytes,
        max_wait_time: @max_wait_time,
        topics: topics,
      )

      batches = response.topics.flat_map {|fetched_topic|
        fetched_topic.partitions.map {|fetched_partition|
          begin
            Protocol.handle_error(fetched_partition.error_code)
          rescue Kafka::OffsetOutOfRange => e
            e.topic = fetched_topic.name
            e.partition = fetched_partition.partition

            raise e
          rescue Kafka::Error => e
            topic = fetched_topic.name
            partition = fetched_partition.partition
            @logger.error "Failed to fetch from #{topic}/#{partition}: #{e.message}"
            raise e
          end

          messages = fetched_partition.messages.map {|message|
            FetchedMessage.new(
              value: message.value,
              key: message.key,
              topic: fetched_topic.name,
              partition: fetched_partition.partition,
              offset: message.offset,
            )
          }

          FetchedBatch.new(
            topic: fetched_topic.name,
            partition: fetched_partition.partition,
            highwater_mark_offset: fetched_partition.highwater_mark_offset,
            messages: messages,
          )
        }
      }

      @output << batches
    end

    def assign(assignments:)
      @assignments = assignments
    end

    def shutdown(**)
      @running = false
    end
  end
end
