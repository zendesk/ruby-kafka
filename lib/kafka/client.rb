require "kafka/broker_pool"
require "kafka/producer"
require "kafka/fetched_message"

module Kafka
  class Client
    DEFAULT_CLIENT_ID = "ruby-kafka"

    # Initializes a new Kafka client.
    #
    # @param seed_brokers [Array<String>] the list of brokers used to initialize
    #   the client.
    #
    # @param client_id [String] the identifier for this application.
    #
    # @param logger [Logger]
    #
    # @param connect_timeout [Integer, nil] the timeout setting for connecting
    #   to brokers. See {BrokerPool#initialize}.
    #
    # @param socket_timeout [Integer, nil] the timeout setting for socket
    #   connections. See {BrokerPool#initialize}.
    #
    # @return [Client]
    def initialize(seed_brokers:, client_id: DEFAULT_CLIENT_ID, logger:, connect_timeout: nil, socket_timeout: nil)
      @logger = logger

      @broker_pool = BrokerPool.new(
        seed_brokers: seed_brokers,
        client_id: client_id,
        logger: logger,
        connect_timeout: connect_timeout,
        socket_timeout: socket_timeout,
      )
    end

    # Builds a new producer.
    #
    # `options` are passed to {Producer#initialize}.
    #
    # @see Producer#initialize
    # @return [Kafka::Producer] the Kafka producer.
    def get_producer(**options)
      Producer.new(broker_pool: @broker_pool, logger: @logger, **options)
    end

    # Fetches a batch of messages from a single partition. Note that it's possible
    # to get back empty batches.
    #
    # The starting point for the fetch can be configured with the `:offset` argument.
    # If you pass a number, the fetch will start at that offset. However, there are
    # two special Symbol values that can be passed instead:
    #
    # * `:earliest` — the first offset in the partition.
    # * `:latest` — the next offset that will be written to, effectively making the
    #   call block until there is a new message in the partition.
    #
    # The Kafka protocol specifies the numeric values of these two options: -2 and -1,
    # respectively. You can also pass in these numbers directly.
    #
    # ## Example
    #
    # When enumerating the messages in a partition, you typically fetch batches
    # sequentially.
    #
    #     offset = :earliest
    #
    #     loop do
    #       messages = kafka.fetch_messages(
    #         topic: "my-topic",
    #         partition: 42,
    #         offset: offset,
    #       )
    #
    #       messages.each do |message|
    #         puts message.offset, message.key, message.value
    #
    #         # Set the next offset that should be read to be the subsequent
    #         # offset.
    #         offset = message.offset + 1
    #       end
    #     end
    #
    # See a working example in `examples/simple-consumer.rb`.
    #
    # @note This API is still alpha level. Don't try to use it in production.
    #
    # @param topic [String] the topic that messages should be fetched from.
    #
    # @param partition [Integer] the partition that messages should be fetched from.
    #
    # @param offset [Integer, Symbol] the offset to start reading from. Default is
    #   the latest offset.
    #
    # @param max_wait_time [Integer] the maximum amount of time to wait before
    #   the server responds.
    #
    # @param min_bytes [Integer] the minimum number of bytes to wait for. If set to
    #   zero, the broker will respond immediately, but the response may be empty.
    #   The default is 1 byte, which means that the broker will respond as soon as
    #   a message is written to the partition.
    #
    # @param max_bytes [Integer] the maximum number of bytes to include in the
    #   response message set. Default is 1 MB. You need to set this higher if you
    #   expect messages to be larger than this.
    #
    # @return [Array<Kafka::FetchedMessage>] the messages returned from the broker.
    def fetch_messages(topic:, partition:, offset: :latest, max_wait_time: 10, min_bytes: 1, max_bytes: 1048576)
      broker = @broker_pool.get_leader(topic, partition)

      offset = resolve_offset(topic, partition, offset, broker)

      options = {
        max_wait_time: max_wait_time * 1000, # Kafka expects ms, not secs
        min_bytes: min_bytes,
        topics: {
          topic => {
            partition => {
              fetch_offset: offset,
              max_bytes: max_bytes,
            }
          }
        }
      }

      response = broker.fetch_messages(**options)

      response.topics.map {|fetched_topic|
        fetched_topic.partitions.map {|fetched_partition|
          Protocol.handle_error(fetched_partition.error_code)

          fetched_partition.messages.map {|offset, message|
            FetchedMessage.new(
              value: message.value,
              key: message.key,
              topic: fetched_topic.name,
              partition: fetched_partition.partition,
              offset: offset,
            )
          }
        }
      }.flatten
    end

    # Lists all topics in the cluster.
    #
    # @return [Array<String>] the list of topic names.
    def topics
      @broker_pool.topics
    end

    def close
      @broker_pool.shutdown
    end

    private

    def resolve_offset(topic, partition, logical_offset, broker)
      if logical_offset == :earliest
        offset = -2
      elsif logical_offset == :latest
        offset = -1
      else
        offset = logical_offset
      end

      # Positive offsets don't need resolving.
      return offset if offset > 0

      @logger.debug "Resolving offset `#{logical_offset}` for #{topic}/#{partition}..."

      response = broker.list_offsets(
        topics: {
          topic => [
            { partition: partition, time: offset, max_offsets: 1 }
          ]
        }
      )

      resolved_offset = response.offset_for(topic, partition)

      @logger.debug "Offset `#{logical_offset}` for #{topic}/#{partition} is #{resolved_offset.inspect}"

      resolved_offset || 0
    end
  end
end
