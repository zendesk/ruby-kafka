require "kafka/broker_pool"
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
    def initialize(seed_brokers:, broker_pool:, logger:)
      if seed_brokers.empty?
        raise ArgumentError, "At least one seed broker must be configured"
      end

      @logger = logger
      @seed_brokers = seed_brokers
      @broker_pool = broker_pool
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
      new_topics = Set.new(topics) - @target_topics

      unless new_topics.empty?
        @logger.info "New topics added to target list: #{new_topics.to_a.join(', ')}"

        @target_topics.merge(new_topics)

        refresh_metadata!
      end
    end

    def api_info(api_key)
      apis.find {|api| api.api_key == api_key }
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

    def get_group_coordinator(group_id:)
      @logger.debug "Getting group coordinator for `#{group_id}`"

      refresh_metadata_if_necessary!

      cluster_info.brokers.each do |broker_info|
        begin
          broker = connect_to_broker(broker_info.node_id)
          response = broker.find_group_coordinator(group_id: group_id)

          Protocol.handle_error(response.error_code)

          coordinator_id = response.coordinator_id

          @logger.debug "Coordinator for group `#{group_id}` is #{coordinator_id}. Connecting..."

          # It's possible that a new broker is introduced to the cluster and
          # becomes the coordinator before we have a chance to refresh_metadata.
          coordinator = begin
            connect_to_broker(coordinator_id)
          rescue Kafka::NoSuchBroker
            @logger.debug "Broker #{coordinator_id} missing from broker cache, refreshing"
            refresh_metadata!
            connect_to_broker(coordinator_id)
          end

          @logger.debug "Connected to coordinator: #{coordinator} for group `#{group_id}`"

          return coordinator
        rescue GroupCoordinatorNotAvailable
          @logger.debug "Coordinator not available; retrying in 1s"
          sleep 1
          retry
        rescue ConnectionError => e
          @logger.error "Failed to get group coordinator info from #{broker}: #{e}"
        end
      end

      raise Kafka::Error, "Failed to find group coordinator"
    end

    def partitions_for(topic)
      add_target_topics([topic])
      refresh_metadata_if_necessary!
      cluster_info.partitions_for(topic)
    rescue Kafka::ProtocolError
      mark_as_stale!
      raise
    end

    def create_topic(name, num_partitions: 1, replication_factor: 1, timeout: 30)
      options = {
        topics: {
          name => {
            num_partitions: num_partitions,
            replication_factor: replication_factor,
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
      end

      @logger.info "Topic `#{name}` was created"
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
                time: offset,
                max_offsets: 1,
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
      cluster_info.topics.map(&:topic_name)
    end

    def disconnect
      @broker_pool.close
    end

    private

    def get_leader_id(topic, partition)
      cluster_info.find_leader_id(topic, partition)
    end

    def cluster_info
      @cluster_info ||= fetch_cluster_info
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
        @logger.info "Fetching cluster metadata from #{node}"

        begin
          broker = @broker_pool.connect(node.hostname, node.port)
          cluster_info = broker.fetch_metadata(topics: @target_topics)

          if cluster_info.brokers.empty?
            @logger.error "No brokers in cluster"
          else
            @logger.info "Discovered cluster metadata; nodes: #{cluster_info.brokers.join(', ')}"

            @stale = false

            return cluster_info
          end
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
          errors << [node, e]
        ensure
          broker.disconnect unless broker.nil?
        end
      end

      error_description = errors.map {|node, exception| "- #{node}: #{exception}" }.join("\n")

      raise ConnectionError, "Could not connect to any of the seed brokers:\n#{error_description}"
    end

    def random_broker
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
  end
end
