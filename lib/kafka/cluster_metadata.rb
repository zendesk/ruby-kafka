module Kafka
  class ClusterMetadata
    def initialize(seed_brokers:, client:, logger:)
      if seed_brokers.empty?
        raise ArgumentError, "At least one seed broker must be configured"
      end

      @logger = logger
      @seed_brokers = seed_brokers
      @client = client
      @metadata = nil
    end

    def mark_as_stale!
      @metadata = nil
    end

    def partitions_for(topic)
      metadata.partitions_for(topic)
    end

    def topics
      metadata.topics.map(&:topic_name)
    end

    def get_leader_id(topic, partition)
      metadata.find_leader_id(topic, partition)
    end

    def find_broker(broker_id)
      metadata.find_broker(broker_id)
    end

    private

    def metadata
      @metadata ||= fetch_metadata
    end

    def fetch_metadata
      @seed_brokers.each do |node|
        @logger.info "Trying to initialize broker pool from node #{node}"

        begin
          host, port = node.split(":", 2)

          broker = @client.connect_to(host, port.to_i)
          metadata = broker.fetch_metadata

          @logger.info "Fetched cluster metadata with broker list: #{metadata.brokers.inspect}"

          return metadata
        rescue Error => e
          @logger.error "Failed to fetch metadata from #{node}: #{e}"
        ensure
          broker.disconnect unless broker.nil?
        end
      end

      raise ConnectionError, "Could not connect to any of the seed brokers: #{@seed_brokers.inspect}"
    end
  end
end
