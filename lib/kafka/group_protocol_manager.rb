require "kafka/round_robin_assignment_strategy"

module Kafka
  class GroupProtocolManager
    class GroupProtocol
      attr_reader :name, :metadata, :assignment_strategy

      def initialize(name:, metadata: '', assignment_strategy:)
        @name = name
        @metadata = metadata
        @assignment_strategy = assignment_strategy.new
      end
    end

    attr_reader :group_protocols

    def initialize(cluster:, group_protocols: [])
      @cluster = cluster
      @group_protocols = group_protocols.map do |protocol|
        GroupProtocol.new(
          name: protocol[:name],
          metadata: protocol[:metadata],
          assignment_strategy: protocol[:assignment_strategy]
        )
      end
      @fallback_protocol = GroupProtocol.new(
        name: 'standard',
        metadata: '',
        assignment_strategy: RoundRobinAssignmentStrategy
      )
      @group_protocols << @fallback_protocol if @group_protocols.empty?
    end

    def assign(group_protocol:, members:, topics:)
      topic_partitions = {}
      topics.each do |topic|
        begin
          topic_partitions[topic] = @cluster.partitions_for(topic).map(&:partition_id)
        rescue UnknownTopicOrPartition
          raise UnknownTopicOrPartition, "unknown topic #{topic}"
        end
      end

      protocol = fetch_selected_protocol(group_protocol)
      protocol.assignment_strategy.assign(
        members: members,
        topics: topic_partitions
      )
    rescue Kafka::LeaderNotAvailable
      sleep 1
      retry
    end

    private

    def fetch_selected_protocol(group_protocol)
      selected_protocol = @group_protocols.find do |protocol|
        protocol.name == group_protocol
      end
      selected_protocol.nil? ? @fallback_protocol : selected_protocol
    end
  end
end
