require "kafka/protocol/member_assignment"

module Kafka

  # A consumer group partition assignment strategy that assigns partitions to
  # consumers in a round-robin fashion.
  class RoundRobinAssignmentStrategy
    def initialize(cluster:)
      @cluster = cluster
    end

    # Assign the topic partitions to the group members.
    #
    # @param members [Array<String>] member ids
    # @return [Hash<String, Protocol::MemberAssignment>] a hash mapping member
    #   ids to assignments.
    def assign(members:)
      group_assignment = {}

      members.each_key do |member_id|
        group_assignment[member_id] = Protocol::MemberAssignment.new
      end
      topic_mapping = members.reduce({}) do |map, member|
        member.last.topics.each do |topic|
          map[topic] ||= []
          map[topic] << member
        end
        map
      end

      topic_mapping.each do |topic, member_array|
        begin
          partitions = @cluster.partitions_for(topic).map(&:partition_id)
        rescue UnknownTopicOrPartition
          raise UnknownTopicOrPartition, "unknown topic #{topic}"
        end

        partitions_per_member = partitions.group_by {|partition_id|
          partition_id % member_array.count
        }.values

        member_array.zip(partitions_per_member).each do |member_id, member_partitions|
          unless member_partitions.nil?
            group_assignment[member_id.first].assign(topic, member_partitions)
          end
        end
      end

      group_assignment
    rescue Kafka::LeaderNotAvailable
      sleep 1
      retry
    end
  end
end
