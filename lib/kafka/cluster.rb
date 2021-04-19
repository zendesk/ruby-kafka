# frozen_string_literal: true

require "kafka/broker_pool"
require "resolv"
require "set"

module Kafka

  # A cluster represents the state of a Kafka cluster. It needs to be initialized
  # with a non-empty list of seed brokers. The first seed broker that the cluster can connect
  # to will be asked for the cluster metadata, which allows the cluster to map topic
  # partitions to the current leader for those partitions.
  class Cluster

    # Initializes a Cluster with a set of seed brokers.
    #
    # The cluster will try to fetch cluster metadata from one of the brokers.
    #
    # @param seed_brokers [Array<URI>]
    # @param broker_pool [Kafka::BrokerPool]
    # @param logger [Logger]
    # @param resolve_seed_brokers [Boolean] See {Kafka::Client#initialize}
    def initialize(seed_brokers:, broker_pool:, logger:, resolve_seed_brokers: false)
      if seed_brokers.empty?
        raise ArgumentError, "At least one seed broker must be configured"
      end

      @logger = TaggedLogger.new(logger)
      @seed_brokers = seed_brokers
      @broker_pool = broker_pool
      @resolve_seed_brokers = resolve_seed_brokers
      @cluster_info = nil
      @stale = true

      # This is the set of topics we need metadata for. If empty, metadata for
      # all topics will be fetched.
      @target_topics = Set.new
    end

    # Adds a list of topics to the target list. Only the topics on this list will
    # be queried for metadata.
    #
    # @param topics [Array<String>]
    # @return [nil]
    def add_target_topics(topics)
      topics = Set.new(topics)
      unless topics.subset?(@target_topics)
        new_topics = topics - @target_topics

        unless new_topics.empty?
          if new_topics.any? { |topic| topic.nil? or topic.empty? }
            raise ArgumentError, "Topic must not be nil or empty"
          end

          @logger.info "New topics added to target list: #{new_topics.to_a.join(', ')}"

          @target_topics.merge(new_topics)

          refresh_metadata!
        end
      end
    end

    def api_info(api_key)
      apis.find {|api| api.api_key == api_key }
    end

    def supports_api?(api_key, version = nil)
      info = api_info(api_key)
      if info.nil?
        return false
      elsif version.nil?
        return true
      else
        return info.version_supported?(version)
      end
    end

    def apis
      @apis ||=
        begin
          response = random_broker.api_versions

          Protocol.handle_error(response.error_code)

          response.apis
        end
    end

    # Clears the list of target topics.
    #
    # @see #add_target_topics
    # @return [nil]
    def clear_target_topics
      @target_topics.clear
      refresh_metadata!
    end

    def mark_as_stale!
      @stale = true
    end

    def refresh_metadata!
      @cluster_info = nil
      cluster_info
    end

    def refresh_metadata_if_necessary!
      refresh_metadata! if @stale
    end

    # Finds the broker acting as the leader of the given topic and partition.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @return [Broker] the broker that's currently leader.
    def get_leader(topic, partition)
      connect_to_broker(get_leader_id(topic, partition))
    end

    # Finds the broker acting as the coordinator of the given group.
    #
    # @param group_id [String]
    # @return [Broker] the broker that's currently coordinator.
    def get_group_coordinator(group_id:)
      @logger.debug "Getting group coordinator for `#{group_id}`"
      refresh_metadata_if_necessary!
      get_coordinator(Kafka::Protocol::COORDINATOR_TYPE_GROUP, group_id)
    end

    # Finds the broker acting as the coordinator of the given transaction.
    #
    # @param transactional_id [String]
    # @return [Broker] the broker that's currently coordinator.
    def get_transaction_coordinator(transactional_id:)
      @logger.debug "Getting transaction coordinator for `#{transactional_id}`"

      refresh_metadata_if_necessary!

      if transactional_id.nil?
        # Get a random_broker
        @logger.debug "Transaction ID is not available. Choose a random broker."
        return random_broker
      else
        get_coordinator(Kafka::Protocol::COORDINATOR_TYPE_TRANSACTION, transactional_id)
      end
    end

    def describe_configs(broker_id, configs = [])
      options = {
        resources: [[Kafka::Protocol::RESOURCE_TYPE_CLUSTER, broker_id.to_s, configs]]
      }

      info = cluster_info.brokers.find {|broker| broker.node_id == broker_id }
      broker = @broker_pool.connect(info.host, info.port, node_id: info.node_id)

      response = broker.describe_configs(**options)

      response.resources.each do |resource|
        Protocol.handle_error(resource.error_code, resource.error_message)
      end

      response.resources.first.configs
    end

    def alter_configs(broker_id, configs = [])
      options = {
        resources: [[Kafka::Protocol::RESOURCE_TYPE_CLUSTER, broker_id.to_s, configs]]
      }

      info = cluster_info.brokers.find {|broker| broker.node_id == broker_id }
      broker = @broker_pool.connect(info.host, info.port, node_id: info.node_id)

      response = broker.alter_configs(**options)

      response.resources.each do |resource|
        Protocol.handle_error(resource.error_code, resource.error_message)
      end

      nil
    end

    def partitions_for(topic)
      add_target_topics([topic])
      refresh_metadata_if_necessary!
      cluster_info.partitions_for(topic)
    rescue Kafka::ProtocolError
      mark_as_stale!
      raise
    end

    def create_topic(name, num_partitions:, replication_factor:, timeout:, config:)
      options = {
        topics: {
          name => {
            num_partitions: num_partitions,
            replication_factor: replication_factor,
            config: config,
          }
        },
        timeout: timeout,
      }

      broker = controller_broker

      @logger.info "Creating topic `#{name}` using controller broker #{broker}"

      response = broker.create_topics(**options)

      response.errors.each do |topic, error_code|
        Protocol.handle_error(error_code)
      end

      begin
        partitions_for(name).each do |info|
          Protocol.handle_error(info.partition_error_code)
        end
      rescue Kafka::LeaderNotAvailable
        @logger.warn "Leader not yet available for `#{name}`, waiting 1s..."
        sleep 1

        retry
      rescue Kafka::UnknownTopicOrPartition
        @logger.warn "Topic `#{name}` not yet created, waiting 1s..."
        sleep 1

        retry
      end

      @logger.info "Topic `#{name}` was created"
    end

    def delete_topic(name, timeout:)
      options = {
        topics: [name],
        timeout: timeout,
      }

      broker = controller_broker

      @logger.info "Deleting topic `#{name}` using controller broker #{broker}"

      response = broker.delete_topics(**options)

      response.errors.each do |topic, error_code|
        Protocol.handle_error(error_code)
      end

      @logger.info "Topic `#{name}` was deleted"
    end

    def describe_topic(name, configs = [])
      options = {
        resources: [[Kafka::Protocol::RESOURCE_TYPE_TOPIC, name, configs]]
      }
      broker = controller_broker

      @logger.info "Fetching topic `#{name}`'s configs using controller broker #{broker}"

      response = broker.describe_configs(**options)

      response.resources.each do |resource|
        Protocol.handle_error(resource.error_code, resource.error_message)
      end
      topic_description = response.resources.first
      topic_description.configs.each_with_object({}) do |config, hash|
        hash[config.name] = config.value
      end
    end

    def alter_topic(name, configs = {})
      options = {
        resources: [[Kafka::Protocol::RESOURCE_TYPE_TOPIC, name, configs]]
      }

      broker = controller_broker

      @logger.info "Altering the config for topic `#{name}` using controller broker #{broker}"

      response = broker.alter_configs(**options)

      response.resources.each do |resource|
        Protocol.handle_error(resource.error_code, resource.error_message)
      end

      nil
    end

    def describe_group(group_id)
      response = get_group_coordinator(group_id: group_id).describe_groups(group_ids: [group_id])
      group = response.groups.first
      Protocol.handle_error(group.error_code)
      group
    end

    def fetch_group_offsets(group_id)
      topics = get_group_coordinator(group_id: group_id)
        .fetch_offsets(group_id: group_id, topics: nil)
        .topics

      topics.each do |_, partitions|
        partitions.each do |_, response|
          Protocol.handle_error(response.error_code)
        end
      end

      topics
    end

    def create_partitions_for(name, num_partitions:, timeout:)
      options = {
        topics: [[name, num_partitions, nil]],
        timeout: timeout
      }

      broker = controller_broker

      @logger.info "Creating #{num_partitions} partition(s) for topic `#{name}` using controller broker #{broker}"

      response = broker.create_partitions(**options)

      response.errors.each do |topic, error_code, error_message|
        Protocol.handle_error(error_code, error_message)
      end
      mark_as_stale!

      @logger.info "Topic `#{name}` was updated"
    end

    def resolve_offsets(topic, partitions, offset)
      add_target_topics([topic])
      refresh_metadata_if_necessary!

      partitions_by_broker = partitions.each_with_object({}) {|partition, hsh|
        broker = get_leader(topic, partition)

        hsh[broker] ||= []
        hsh[broker] << partition
      }

      if offset == :earliest
        offset = -2
      elsif offset == :latest
        offset = -1
      end

      offsets = {}

      partitions_by_broker.each do |broker, broker_partitions|
        response = broker.list_offsets(
          topics: {
            topic => broker_partitions.map {|partition|
              {
                partition: partition,
                time: offset
              }
            }
          }
        )

        broker_partitions.each do |partition|
          offsets[partition] = response.offset_for(topic, partition)
        end
      end

      offsets
    rescue Kafka::ProtocolError
      mark_as_stale!
      raise
    end

    def resolve_offset(topic, partition, offset)
      resolve_offsets(topic, [partition], offset).fetch(partition)
    end

    def topics
      refresh_metadata_if_necessary!
      cluster_info.topics.select do |topic|
        topic.topic_error_code == 0
      end.map(&:topic_name)
    end

    # Lists all topics in the cluster.
    def list_topics
      response = random_broker.fetch_metadata(topics: nil)
      response.topics.select do |topic|
        topic.topic_error_code == 0
      end.map(&:topic_name)
    end

    def list_groups
      refresh_metadata_if_necessary!
      cluster_info.brokers.map do |broker|
        response = connect_to_broker(broker.node_id).list_groups
        Protocol.handle_error(response.error_code)
        response.groups.map(&:group_id)
      end.flatten.uniq
    end

    def disconnect
      @broker_pool.close
    end

    def cluster_info
      @cluster_info ||= fetch_cluster_info
    end

    private

    def get_leader_id(topic, partition)
      cluster_info.find_leader_id(topic, partition)
    end

    # Fetches the cluster metadata.
    #
    # This is used to update the partition leadership information, among other things.
    # The methods will go through each node listed in `seed_brokers`, connecting to the
    # first one that is available. This node will be queried for the cluster metadata.
    #
    # @raise [ConnectionError] if none of the nodes in `seed_brokers` are available.
    # @return [Protocol::MetadataResponse] the cluster metadata.
    def fetch_cluster_info
      errors = []
      @seed_brokers.shuffle.each do |node|
        (@resolve_seed_brokers ? Resolv.getaddresses(node.hostname).shuffle : [node.hostname]).each do |hostname_or_ip|
          node_info = node.to_s
          node_info << " (#{hostname_or_ip})" if node.hostname != hostname_or_ip
          @logger.info "Fetching cluster metadata from #{node_info}"

          begin
            broker = @broker_pool.connect(hostname_or_ip, node.port)
            cluster_info = broker.fetch_metadata(topics: @target_topics)

            if cluster_info.brokers.empty?
              @logger.error "No brokers in cluster"
            else
              @logger.info "Discovered cluster metadata; nodes: #{cluster_info.brokers.join(', ')}"

              @stale = false

              return cluster_info
            end
          rescue Error => e
            @logger.error "Failed to fetch metadata from #{node_info}: #{e}"
            errors << [node_info, e]
          ensure
            broker.disconnect unless broker.nil?
          end
        end
      end

      error_description = errors.map {|node_info, exception| "- #{node_info}: #{exception}" }.join("\n")

      raise ConnectionError, "Could not connect to any of the seed brokers:\n#{error_description}"
    end

    def random_broker
      refresh_metadata_if_necessary!
      node_id = cluster_info.brokers.sample.node_id
      connect_to_broker(node_id)
    end

    def connect_to_broker(broker_id)
      info = cluster_info.find_broker(broker_id)

      @broker_pool.connect(info.host, info.port, node_id: info.node_id)
    end

    def controller_broker
      connect_to_broker(cluster_info.controller_id)
    end

    def get_coordinator(coordinator_type, coordinator_key)
      cluster_info.brokers.each do |broker_info|
        begin
          broker = connect_to_broker(broker_info.node_id)
          response = broker.find_coordinator(
            coordinator_type: coordinator_type,
            coordinator_key: coordinator_key
          )

          Protocol.handle_error(response.error_code, response.error_message)

          coordinator_id = response.coordinator_id

          @logger.debug "Coordinator for `#{coordinator_key}` is #{coordinator_id}. Connecting..."

          # It's possible that a new broker is introduced to the cluster and
          # becomes the coordinator before we have a chance to refresh_metadata.
          coordinator = begin
            connect_to_broker(coordinator_id)
          rescue Kafka::NoSuchBroker
            @logger.debug "Broker #{coordinator_id} missing from broker cache, refreshing"
            refresh_metadata!
            connect_to_broker(coordinator_id)
          end

          @logger.debug "Connected to coordinator: #{coordinator} for `#{coordinator_key}`"

          return coordinator
        rescue CoordinatorNotAvailable
          @logger.debug "Coordinator not available; retrying in 1s"
          sleep 1
          retry
        rescue ConnectionError => e
          @logger.error "Failed to get coordinator info from #{broker}: #{e}"
        end
      end

      raise Kafka::Error, "Failed to find coordinator"
    end
  end
end
