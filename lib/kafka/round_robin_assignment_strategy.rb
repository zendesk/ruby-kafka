require "kafka/protocol/member_assignment"

module Kafka
  # A consumer group partition assignment strategy that assigns partitions to
  # consumers in a round-robin fashion.
  class RoundRobinAssignmentStrategy
    # Assign the topic partitions to the group members.
    #
    # @param members [Hash<String, String>] a hash mapping member ids to member
    # metadata
    # @param topics [Hash<String, Array<String>>] a hash mapping topics to list of
    # topic partitions
    # @return [Hash<String, Protocol::MemberAssignment>] a hash mapping member
    #   ids to assignments.
    def assign(members:, topics:)
      group_assignment = {}

      members.keys.each do |member_id|
        group_assignment[member_id] = Protocol::MemberAssignment.new
      end

      topics.each do |topic, partitions|
        partitions_per_member = partitions.group_by {|partition_id|
          partition_id % members.count
        }.values

        members.keys.zip(partitions_per_member).each do |member_id, member_partitions|
          unless member_partitions.nil?
            group_assignment[member_id].assign(topic, member_partitions)
          end
        end
      end

      group_assignment
    end
  end
end
