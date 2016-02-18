require "kafka/cluster"
require "kafka/producer"
require "kafka/fetched_message"
require "kafka/fetch_operation"

module Kafka
  class Client
    DEFAULT_CLIENT_ID = "ruby-kafka"
    DEFAULT_LOGGER = Logger.new("/dev/null")

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
    def initialize(seed_brokers:, client_id: DEFAULT_CLIENT_ID, logger: DEFAULT_LOGGER, connect_timeout: nil, socket_timeout: nil)
      @logger = logger

      broker_pool = BrokerPool.new(
        client_id: client_id,
        connect_timeout: connect_timeout,
        socket_timeout: socket_timeout,
        logger: logger,
      )

      @cluster = Cluster.new(
        seed_brokers: seed_brokers,
        broker_pool: broker_pool,
        logger: logger,
      )
    end

    # Builds a new producer.
    #
    # `options` are passed to {Producer#initialize}.
    #
    # @see Producer#initialize
    # @return [Kafka::Producer] the Kafka producer.
    def get_producer(**options)
      Producer.new(cluster: @cluster, logger: @logger, **options)
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
    #   the server responds, in seconds.
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
    def fetch_messages(topic:, partition:, offset: :latest, max_wait_time: 5, min_bytes: 1, max_bytes: 1048576)
      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: min_bytes,
        max_wait_time: max_wait_time,
      )

      operation.fetch_from_partition(topic, partition, offset: offset, max_bytes: max_bytes)

      operation.execute
    end

    # Lists all topics in the cluster.
    #
    # @return [Array<String>] the list of topic names.
    def topics
      @cluster.topics
    end

    # Counts the number of partitions in a topic.
    #
    # @param topic [String]
    # @return [Integer] the number of partitions in the topic.
    def partitions_for(topic)
      @cluster.partitions_for(topic).count
    end

    def close
      @cluster.disconnect
    end
  end
end
