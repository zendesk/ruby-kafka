# frozen_string_literal: true

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

      topic_partitions = topics.flat_map do |topic|
        begin
          partitions = @cluster.partitions_for(topic).map(&:partition_id)
        rescue UnknownTopicOrPartition
          raise UnknownTopicOrPartition, "unknown topic #{topic}"
        end
        Array.new(partitions.count) { topic }.zip(partitions)
      end

      partitions_per_member = [1, (topic_partitions.count / members.count)].max
      partitions_for_each_member = topic_partitions.sort.each_slice(partitions_per_member).to_a

      if partitions_for_each_member.count > members.count
        leftovers = partitions_for_each_member.pop
        partitions_for_each_member[0] = partitions_for_each_member[0] + leftovers
      end

      members.zip(partitions_for_each_member).each do |member_id, member_partitions|
        unless member_partitions.nil?
          member_partitions.group_by(&:first).each do |topic, values|
            partition_ids = values.map { |(topic, partition_id)| partition_id }
            group_assignment[member_id].assign(topic, partition_ids)
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
