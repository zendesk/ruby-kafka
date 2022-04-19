# coding: utf-8
# frozen_string_literal: true

require "kafka/ssl_context"
require "kafka/cluster"
require "kafka/transaction_manager"
require "kafka/broker_info"
require "kafka/producer"
require "kafka/consumer"
require "kafka/heartbeat"
require "kafka/broker_uri"
require "kafka/async_producer"
require "kafka/fetched_message"
require "kafka/fetch_operation"
require "kafka/connection_builder"
require "kafka/instrumenter"
require "kafka/sasl_authenticator"
require "kafka/tagged_logger"

module Kafka
  class Client
    # Initializes a new Kafka client.
    #
    # @param seed_brokers [Array<String>, String] the list of brokers used to initialize
    #   the client. Either an Array of connections, or a comma separated string of connections.
    #   A connection can either be a string of "host:port" or a full URI with a scheme.
    #   If there's a scheme it's ignored and only host/port are used.
    #
    # @param client_id [String] the identifier for this application.
    #
    # @param logger [Logger] the logger that should be used by the client.
    #
    # @param connect_timeout [Integer, nil] the timeout setting for connecting
    #   to brokers. See {BrokerPool#initialize}.
    #
    # @param socket_timeout [Integer, nil] the timeout setting for socket
    #   connections. See {BrokerPool#initialize}.
    #
    # @param ssl_ca_cert [String, Array<String>, nil] a PEM encoded CA cert, or an Array of
    #   PEM encoded CA certs, to use with an SSL connection.
    #
    # @param ssl_ca_cert_file_path [String, Array<String>, nil] a path on the filesystem, or an
    #    Array of paths, to PEM encoded CA cert(s) to use with an SSL connection.
    #
    # @param ssl_client_cert [String, nil] a PEM encoded client cert to use with an
    #   SSL connection. Must be used in combination with ssl_client_cert_key.
    #
    # @param ssl_client_cert_key [String, nil] a PEM encoded client cert key to use with an
    #   SSL connection. Must be used in combination with ssl_client_cert.
    #
    # @param ssl_client_cert_key_password [String, nil] the password required to read the
    #   ssl_client_cert_key. Must be used in combination with ssl_client_cert_key.
    #
    # @param sasl_gssapi_principal [String, nil] a KRB5 principal
    #
    # @param sasl_gssapi_keytab [String, nil] a KRB5 keytab filepath
    #
    # @param sasl_scram_username [String, nil] SCRAM username
    #
    # @param sasl_scram_password [String, nil] SCRAM password
    #
    # @param sasl_scram_mechanism [String, nil] Scram mechanism, either "sha256" or "sha512"
    #
    # @param sasl_over_ssl [Boolean] whether to enforce SSL with SASL
    #
    # @param ssl_ca_certs_from_system [Boolean] whether to use the CA certs from the
    #   system's default certificate store.
    #
    # @param partitioner [Partitioner, nil] the partitioner that should be used by the client.
    #
    # @param sasl_oauth_token_provider [Object, nil] OAuthBearer Token Provider instance that
    #   implements method token. See {Sasl::OAuth#initialize}
    #
    # @param ssl_verify_hostname [Boolean, true] whether to verify that the host serving
    #   the SSL certificate and the signing chain of the certificate have the correct domains
    #   based on the CA certificate
    #
    # @param resolve_seed_brokers [Boolean] whether to resolve each hostname of the seed brokers.
    #   If a broker is resolved to multiple IP addresses, the client tries to connect to each
    #   of the addresses until it can connect.
    #
    # @return [Client]
    def initialize(seed_brokers:, client_id: "ruby-kafka", logger: nil, connect_timeout: nil, socket_timeout: nil,
                   ssl_ca_cert_file_path: nil, ssl_ca_cert: nil, ssl_client_cert: nil, ssl_client_cert_key: nil,
                   ssl_client_cert_key_password: nil, ssl_client_cert_chain: nil, sasl_gssapi_principal: nil,
                   sasl_gssapi_keytab: nil, sasl_plain_authzid: '', sasl_plain_username: nil, sasl_plain_password: nil,
                   sasl_scram_username: nil, sasl_scram_password: nil, sasl_scram_mechanism: nil,
                   sasl_aws_msk_iam_access_key_id: nil,
                   sasl_aws_msk_iam_secret_key_id: nil,
                   sasl_aws_msk_iam_aws_region: nil,
                   sasl_aws_msk_iam_session_token: nil,
                   sasl_over_ssl: true, ssl_ca_certs_from_system: false, partitioner: nil, sasl_oauth_token_provider: nil, ssl_verify_hostname: true,
                   resolve_seed_brokers: false)
      @logger = TaggedLogger.new(logger)
      @instrumenter = Instrumenter.new(client_id: client_id)
      @seed_brokers = normalize_seed_brokers(seed_brokers)
      @resolve_seed_brokers = resolve_seed_brokers

      ssl_context = SslContext.build(
        ca_cert_file_path: ssl_ca_cert_file_path,
        ca_cert: ssl_ca_cert,
        client_cert: ssl_client_cert,
        client_cert_key: ssl_client_cert_key,
        client_cert_key_password: ssl_client_cert_key_password,
        client_cert_chain: ssl_client_cert_chain,
        ca_certs_from_system: ssl_ca_certs_from_system,
        verify_hostname: ssl_verify_hostname
      )

      sasl_authenticator = SaslAuthenticator.new(
        sasl_gssapi_principal: sasl_gssapi_principal,
        sasl_gssapi_keytab: sasl_gssapi_keytab,
        sasl_plain_authzid: sasl_plain_authzid,
        sasl_plain_username: sasl_plain_username,
        sasl_plain_password: sasl_plain_password,
        sasl_scram_username: sasl_scram_username,
        sasl_scram_password: sasl_scram_password,
        sasl_scram_mechanism: sasl_scram_mechanism,
        sasl_aws_msk_iam_access_key_id: sasl_aws_msk_iam_access_key_id,
        sasl_aws_msk_iam_secret_key_id: sasl_aws_msk_iam_secret_key_id,
        sasl_aws_msk_iam_aws_region: sasl_aws_msk_iam_aws_region,
        sasl_aws_msk_iam_session_token: sasl_aws_msk_iam_session_token,
        sasl_oauth_token_provider: sasl_oauth_token_provider,
        logger: @logger
      )

      if sasl_authenticator.enabled? && sasl_over_ssl && ssl_context.nil?
        raise ArgumentError, "SASL authentication requires that SSL is configured"
      end

      @connection_builder = ConnectionBuilder.new(
        client_id: client_id,
        connect_timeout: connect_timeout,
        socket_timeout: socket_timeout,
        ssl_context: ssl_context,
        logger: @logger,
        instrumenter: @instrumenter,
        sasl_authenticator: sasl_authenticator
      )

      @cluster = initialize_cluster
      @partitioner = partitioner || Partitioner.new
    end

    # Delivers a single message to the Kafka cluster.
    #
    # **Note:** Only use this API for low-throughput scenarios. If you want to deliver
    # many messages at a high rate, or if you want to configure the way messages are
    # sent, use the {#producer} or {#async_producer} APIs instead.
    #
    # @param value [String, nil] the message value.
    # @param key [String, nil] the message key.
    # @param headers [Hash<String, String>] the headers for the message.
    # @param topic [String] the topic that the message should be written to.
    # @param partition [Integer, nil] the partition that the message should be written
    #   to, or `nil` if either `partition_key` is passed or the partition should be
    #   chosen at random.
    # @param partition_key [String] a value used to deterministically choose a
    #   partition to write to.
    # @param retries [Integer] the number of times to retry the delivery before giving
    #   up.
    # @return [nil]
    def deliver_message(value, key: nil, headers: {}, topic:, partition: nil, partition_key: nil, retries: 1)
      create_time = Time.now

      # We want to fail fast if `topic` isn't a String
      topic = topic.to_str

      message = PendingMessage.new(
        value: value,
        key: key,
        headers: headers,
        topic: topic,
        partition: partition,
        partition_key: partition_key,
        create_time: create_time
      )

      if partition.nil?
        partition_count = @cluster.partitions_for(topic).count
        partition = @partitioner.call(partition_count, message)
      end

      buffer = MessageBuffer.new

      buffer.write(
        value: message.value,
        key: message.key,
        headers: message.headers,
        topic: message.topic,
        partition: partition,
        create_time: message.create_time,
      )

      @cluster.add_target_topics([topic])

      compressor = Compressor.new(
        instrumenter: @instrumenter,
      )

      transaction_manager = TransactionManager.new(
        cluster: @cluster,
        logger: @logger,
        idempotent: false,
        transactional: false
      )

      operation = ProduceOperation.new(
        cluster: @cluster,
        transaction_manager: transaction_manager,
        buffer: buffer,
        required_acks: 1,
        ack_timeout: 10,
        compressor: compressor,
        logger: @logger,
        instrumenter: @instrumenter,
      )

      attempt = 1

      begin
        @cluster.refresh_metadata_if_necessary!

        operation.execute

        unless buffer.empty?
          raise DeliveryFailed.new(nil, [message])
        end
      rescue Kafka::Error => e
        @cluster.mark_as_stale!

        if attempt >= (retries + 1)
          raise
        else
          attempt += 1
          @logger.warn "Error while delivering message, #{e.class}: #{e.message}; retrying after 1s..."

          sleep 1

          retry
        end
      end
    end

    # Initializes a new Kafka producer.
    #
    # @param ack_timeout [Integer] The number of seconds a broker can wait for
    #   replicas to acknowledge a write before responding with a timeout.
    #
    # @param required_acks [Integer, Symbol] The number of replicas that must acknowledge
    #   a write, or `:all` if all in-sync replicas must acknowledge.
    #
    # @param max_retries [Integer] the number of retries that should be attempted
    #   before giving up sending messages to the cluster. Does not include the
    #   original attempt.
    #
    # @param retry_backoff [Integer] the number of seconds to wait between retries.
    #
    # @param max_buffer_size [Integer] the number of messages allowed in the buffer
    #   before new writes will raise {BufferOverflow} exceptions.
    #
    # @param max_buffer_bytesize [Integer] the maximum size of the buffer in bytes.
    #   attempting to produce messages when the buffer reaches this size will
    #   result in {BufferOverflow} being raised.
    #
    # @param compression_codec [Symbol, nil] the name of the compression codec to
    #   use, or nil if no compression should be performed. Valid codecs: `:snappy`,
    #   `:gzip`, `:lz4`, `:zstd`
    #
    # @param compression_threshold [Integer] the number of messages that needs to
    #   be in a message set before it should be compressed. Note that message sets
    #   are per-partition rather than per-topic or per-producer.
    #
    # @param interceptors [Array<Object>] a list of producer interceptors the implement
    #   `call(Kafka::PendingMessage)`.
    #
    # @return [Kafka::Producer] the Kafka producer.
    def producer(
      compression_codec: nil,
      compression_threshold: 1,
      ack_timeout: 5,
      required_acks: :all,
      max_retries: 2,
      retry_backoff: 1,
      max_buffer_size: 1000,
      max_buffer_bytesize: 10_000_000,
      idempotent: false,
      transactional: false,
      transactional_id: nil,
      transactional_timeout: 60,
      interceptors: []
    )
      cluster = initialize_cluster
      compressor = Compressor.new(
        codec_name: compression_codec,
        threshold: compression_threshold,
        instrumenter: @instrumenter,
      )

      transaction_manager = TransactionManager.new(
        cluster: cluster,
        logger: @logger,
        idempotent: idempotent,
        transactional: transactional,
        transactional_id: transactional_id,
        transactional_timeout: transactional_timeout,
      )

      Producer.new(
        cluster: cluster,
        transaction_manager: transaction_manager,
        logger: @logger,
        instrumenter: @instrumenter,
        compressor: compressor,
        ack_timeout: ack_timeout,
        required_acks: required_acks,
        max_retries: max_retries,
        retry_backoff: retry_backoff,
        max_buffer_size: max_buffer_size,
        max_buffer_bytesize: max_buffer_bytesize,
        partitioner: @partitioner,
        interceptors: interceptors
      )
    end

    # Creates a new AsyncProducer instance.
    #
    # All parameters allowed by {#producer} can be passed. In addition to this,
    # a few extra parameters can be passed when creating an async producer.
    #
    # @param max_queue_size [Integer] the maximum number of messages allowed in
    #   the queue.
    # @param delivery_threshold [Integer] if greater than zero, the number of
    #   buffered messages that will automatically trigger a delivery.
    # @param delivery_interval [Integer] if greater than zero, the number of
    #   seconds between automatic message deliveries.
    #
    # @see AsyncProducer
    # @return [AsyncProducer]
    def async_producer(delivery_interval: 0, delivery_threshold: 0, max_queue_size: 1000, max_retries: -1, retry_backoff: 0, **options)
      sync_producer = producer(**options)

      AsyncProducer.new(
        sync_producer: sync_producer,
        delivery_interval: delivery_interval,
        delivery_threshold: delivery_threshold,
        max_queue_size: max_queue_size,
        max_retries: max_retries,
        retry_backoff: retry_backoff,
        instrumenter: @instrumenter,
        logger: @logger,
      )
    end

    # Creates a new Kafka consumer.
    #
    # @param group_id [String] the id of the group that the consumer should join.
    # @param session_timeout [Integer] the number of seconds after which, if a client
    #   hasn't contacted the Kafka cluster, it will be kicked out of the group.
    # @param offset_commit_interval [Integer] the interval between offset commits,
    #   in seconds.
    # @param offset_commit_threshold [Integer] the number of messages that can be
    #   processed before their offsets are committed. If zero, offset commits are
    #   not triggered by message processing.
    # @param heartbeat_interval [Integer] the interval between heartbeats; must be less
    #   than the session window.
    # @param offset_retention_time [Integer] the time period that committed
    #   offsets will be retained, in seconds. Defaults to the broker setting.
    # @param fetcher_max_queue_size [Integer] max number of items in the fetch queue that
    #   are stored for further processing. Note, that each item in the queue represents a
    #   response from a single broker.
    # @param refresh_topic_interval [Integer] interval of refreshing the topic list.
    #   If it is 0, the topic list won't be refreshed (default)
    #   If it is n (n > 0), the topic list will be refreshed every n seconds
    # @param interceptors [Array<Object>] a list of consumer interceptors that implement
    #   `call(Kafka::FetchedBatch)`.
    # @param assignment_strategy [Object] a partition assignment strategy that
    #   implements `protocol_type()`, `user_data()`, and `assign(members:, partitions:)`
    # @return [Consumer]
    def consumer(
        group_id:,
        session_timeout: 30,
        rebalance_timeout: 60,
        offset_commit_interval: 10,
        offset_commit_threshold: 0,
        heartbeat_interval: 10,
        offset_retention_time: nil,
        fetcher_max_queue_size: 100,
        refresh_topic_interval: 0,
        interceptors: [],
        assignment_strategy: nil
    )
      cluster = initialize_cluster

      instrumenter = DecoratingInstrumenter.new(@instrumenter, {
        group_id: group_id,
      })

      # The Kafka protocol expects the retention time to be in ms.
      retention_time = (offset_retention_time && offset_retention_time * 1_000) || -1

      group = ConsumerGroup.new(
        cluster: cluster,
        logger: @logger,
        group_id: group_id,
        session_timeout: session_timeout,
        rebalance_timeout: rebalance_timeout,
        retention_time: retention_time,
        instrumenter: instrumenter,
        assignment_strategy: assignment_strategy
      )

      fetcher = Fetcher.new(
        cluster: initialize_cluster,
        group: group,
        logger: @logger,
        instrumenter: instrumenter,
        max_queue_size: fetcher_max_queue_size
      )

      offset_manager = OffsetManager.new(
        cluster: cluster,
        group: group,
        fetcher: fetcher,
        logger: @logger,
        commit_interval: offset_commit_interval,
        commit_threshold: offset_commit_threshold,
        offset_retention_time: offset_retention_time
      )

      heartbeat = Heartbeat.new(
        group: group,
        interval: heartbeat_interval,
        instrumenter: instrumenter
      )

      Consumer.new(
        cluster: cluster,
        logger: @logger,
        instrumenter: instrumenter,
        group: group,
        offset_manager: offset_manager,
        fetcher: fetcher,
        session_timeout: session_timeout,
        heartbeat: heartbeat,
        refresh_topic_interval: refresh_topic_interval,
        interceptors: interceptors
      )
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
    def fetch_messages(topic:, partition:, offset: :latest, max_wait_time: 5, min_bytes: 1, max_bytes: 1048576, retries: 1)
      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: min_bytes,
        max_bytes: max_bytes,
        max_wait_time: max_wait_time,
      )

      operation.fetch_from_partition(topic, partition, offset: offset, max_bytes: max_bytes)

      attempt = 1

      begin
        operation.execute.flat_map {|batch| batch.messages }
      rescue Kafka::Error => e
        @cluster.mark_as_stale!

        if attempt >= (retries + 1)
          raise
        else
          attempt += 1
          @logger.warn "Error while fetching messages, #{e.class}: #{e.message}; retrying..."
          retry
        end
      end
    end

    # Enumerate all messages in a topic.
    #
    # @param topic [String] the topic to consume messages from.
    #
    # @param start_from_beginning [Boolean] whether to start from the beginning
    #   of the topic or just subscribe to new messages being produced.
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
    # @return [nil]
    def each_message(topic:, start_from_beginning: true, max_wait_time: 5, min_bytes: 1, max_bytes: 1048576, &block)
      default_offset ||= start_from_beginning ? :earliest : :latest
      offsets = Hash.new { default_offset }

      loop do
        operation = FetchOperation.new(
          cluster: @cluster,
          logger: @logger,
          min_bytes: min_bytes,
          max_wait_time: max_wait_time,
        )

        @cluster.partitions_for(topic).map(&:partition_id).each do |partition|
          partition_offset = offsets[partition]
          operation.fetch_from_partition(topic, partition, offset: partition_offset, max_bytes: max_bytes)
        end

        batches = operation.execute

        batches.each do |batch|
          batch.messages.each(&block)
          offsets[batch.partition] = batch.last_offset + 1 unless batch.unknown_last_offset?
        end
      end
    end

    # Describe broker configs
    #
    # @param broker_id [int] the id of the broker
    # @param configs [Array] array of config keys.
    # @return [Array<Kafka::Protocol::DescribeConfigsResponse::ConfigEntry>]
    def describe_configs(broker_id, configs = [])
      @cluster.describe_configs(broker_id, configs)
    end

    # Alter broker configs
    #
    # @param broker_id [int] the id of the broker
    # @param configs [Array] array of config strings.
    # @return [nil]
    def alter_configs(broker_id, configs = [])
      @cluster.alter_configs(broker_id, configs)
    end

    # Creates a topic in the cluster.
    #
    # @example Creating a topic with log compaction
    #   # Enable log compaction:
    #   config = { "cleanup.policy" => "compact" }
    #
    #   # Create the topic:
    #   kafka.create_topic("dns-mappings", config: config)
    #
    # @param name [String] the name of the topic.
    # @param num_partitions [Integer] the number of partitions that should be created
    #   in the topic.
    # @param replication_factor [Integer] the replication factor of the topic.
    # @param timeout [Integer] a duration of time to wait for the topic to be
    #   completely created.
    # @param config [Hash] topic configuration entries. See
    #   [the Kafka documentation](https://kafka.apache.org/documentation/#topicconfigs)
    #   for more information.
    # @raise [Kafka::TopicAlreadyExists] if the topic already exists.
    # @return [nil]
    def create_topic(name, num_partitions: 1, replication_factor: 1, timeout: 30, config: {})
      @cluster.create_topic(
        name,
        num_partitions: num_partitions,
        replication_factor: replication_factor,
        timeout: timeout,
        config: config,
      )
    end

    # Delete a topic in the cluster.
    #
    # @param name [String] the name of the topic.
    # @param timeout [Integer] a duration of time to wait for the topic to be
    #   completely marked deleted.
    # @return [nil]
    def delete_topic(name, timeout: 30)
      @cluster.delete_topic(name, timeout: timeout)
    end

    # Describe the configuration of a topic.
    #
    # Retrieves the topic configuration from the Kafka brokers. Configuration names
    # refer to [Kafka's topic-level configs](https://kafka.apache.org/documentation/#topicconfigs).
    #
    # @note This is an alpha level API and is subject to change.
    #
    # @example Describing the cleanup policy config of a topic
    #   kafka = Kafka.new(["kafka1:9092"])
    #   kafka.describe_topic("my-topic", ["cleanup.policy"])
    #   #=> { "cleanup.policy" => "delete" }
    #
    # @param name [String] the name of the topic.
    # @param configs [Array<String>] array of desired config names.
    # @return [Hash<String, String>]
    def describe_topic(name, configs = [])
      @cluster.describe_topic(name, configs)
    end

    # Alter the configuration of a topic.
    #
    # Configuration keys must match
    # [Kafka's topic-level configs](https://kafka.apache.org/documentation/#topicconfigs).
    #
    # @note This is an alpha level API and is subject to change.
    #
    # @example Describing the cleanup policy config of a topic
    #   kafka = Kafka.new(["kafka1:9092"])
    #   kafka.alter_topic("my-topic", "cleanup.policy" => "delete", "max.message.byte" => "100000")
    #
    # @param name [String] the name of the topic.
    # @param configs [Hash<String, String>] hash of desired config keys and values.
    # @return [nil]
    def alter_topic(name, configs = {})
      @cluster.alter_topic(name, configs)
    end

    # Describe a consumer group
    #
    # @param group_id [String] the id of the consumer group
    # @return [Kafka::Protocol::DescribeGroupsResponse::Group]
    def describe_group(group_id)
      @cluster.describe_group(group_id)
    end

    # Fetch all committed offsets for a consumer group
    #
    # @param group_id [String] the id of the consumer group
    # @return [Hash<String, Hash<Integer, Kafka::Protocol::OffsetFetchResponse::PartitionOffsetInfo>>]
    def fetch_group_offsets(group_id)
      @cluster.fetch_group_offsets(group_id)
    end

    # Create partitions for a topic.
    #
    # @param name [String] the name of the topic.
    # @param num_partitions [Integer] the number of desired partitions for
    # the topic
    # @param timeout [Integer] a duration of time to wait for the new
    # partitions to be added.
    # @return [nil]
    def create_partitions_for(name, num_partitions: 1, timeout: 30)
      @cluster.create_partitions_for(name, num_partitions: num_partitions, timeout: timeout)
    end

    # Lists all topics in the cluster.
    #
    # @return [Array<String>] the list of topic names.
    def topics
      attempts = 0
      begin
        attempts += 1
        @cluster.list_topics
      rescue Kafka::ConnectionError
        @cluster.mark_as_stale!
        retry unless attempts > 1
        raise
      end
    end

    # Lists all consumer groups in the cluster
    #
    # @return [Array<String>] the list of group ids
    def groups
      @cluster.list_groups
    end

    def has_topic?(topic)
      @cluster.clear_target_topics
      @cluster.add_target_topics([topic])
      @cluster.topics.include?(topic)
    end

    # Counts the number of partitions in a topic.
    #
    # @param topic [String]
    # @return [Integer] the number of partitions in the topic.
    def partitions_for(topic)
      @cluster.partitions_for(topic).count
    end

    # Counts the number of replicas for a topic's partition
    #
    # @param topic [String]
    # @return [Integer] the number of replica nodes for the topic's partition
    def replica_count_for(topic)
      @cluster.partitions_for(topic).first.replicas.count
    end

    # Retrieve the offset of the last message in a partition. If there are no
    # messages in the partition -1 is returned.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @return [Integer] the offset of the last message in the partition, or -1 if
    #   there are no messages in the partition.
    def last_offset_for(topic, partition)
      # The offset resolution API will return the offset of the "next" message to
      # be written when resolving the "latest" offset, so we subtract one.
      @cluster.resolve_offset(topic, partition, :latest) - 1
    end

    # Retrieve the offset of the last message in each partition of the specified topics.
    #
    # @param topics [Array<String>] topic names.
    # @return [Hash<String, Hash<Integer, Integer>>]
    # @example
    #   last_offsets_for('topic-1', 'topic-2') # =>
    #   # {
    #   #   'topic-1' => { 0 => 100, 1 => 100 },
    #   #   'topic-2' => { 0 => 100, 1 => 100 }
    #   # }
    def last_offsets_for(*topics)
      @cluster.add_target_topics(topics)
      topics.map {|topic|
        partition_ids = @cluster.partitions_for(topic).collect(&:partition_id)
        partition_offsets = @cluster.resolve_offsets(topic, partition_ids, :latest)
        [topic, partition_offsets.collect { |k, v| [k, v - 1] }.to_h]
      }.to_h
    end

    # Check whether current cluster supports a specific version or not
    #
    # @param api_key [Integer] API key.
    # @param version [Integer] API version.
    # @return [Boolean]
    def supports_api?(api_key, version = nil)
      @cluster.supports_api?(api_key, version)
    end

    def apis
      @cluster.apis
    end

    # List all brokers in the cluster.
    #
    # @return [Array<Kafka::BrokerInfo>] the list of brokers.
    def brokers
      @cluster.cluster_info.brokers
    end

    # The current controller broker in the cluster.
    #
    # @return [Kafka::BrokerInfo] information on the controller broker.
    def controller_broker
      brokers.find {|broker| broker.node_id == @cluster.cluster_info.controller_id }
    end

    # Closes all connections to the Kafka brokers and frees up used resources.
    #
    # @return [nil]
    def close
      @cluster.disconnect
    end

    private

    def initialize_cluster
      broker_pool = BrokerPool.new(
        connection_builder: @connection_builder,
        logger: @logger,
      )

      Cluster.new(
        seed_brokers: @seed_brokers,
        broker_pool: broker_pool,
        logger: @logger,
        resolve_seed_brokers: @resolve_seed_brokers,
      )
    end

    def normalize_seed_brokers(seed_brokers)
      if seed_brokers.is_a?(String)
        seed_brokers = seed_brokers.split(",")
      end

      seed_brokers.map {|str| BrokerUri.parse(str) }
    end
  end
end
