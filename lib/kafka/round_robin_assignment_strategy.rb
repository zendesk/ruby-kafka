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
    # @param topics [Array<String>] topics
    # @return [Hash<String, Protocol::MemberAssignment>] a hash mapping member
    #   ids to assignments.
    def assign(members:, topics:)
      group_assignment = {}

      members.each do |member_id|
        group_assignment[member_id] = Protocol::MemberAssignment.new
      end

      topics.each do |topic|
        partitions = @cluster.partitions_for(topic).map(&:partition_id)

        partitions_per_member = partitions.group_by {|partition_id|
          partition_id % members.count
        }.values

        members.zip(partitions_per_member).each do |member_id, member_partitions|
          group_assignment[member_id].assign(topic, member_partitions)
        end
      end

      group_assignment
    end
  end
end
