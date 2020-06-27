# frozen_string_literal: true

require "kafka/protocol/member_assignment"

module Kafka

  # A consumer group partition assignment strategy that assigns partitions to
  # consumers in a round-robin fashion.
  class RoundRobinAssignmentStrategy
    def initialize(cluster:)
      @cluster = cluster
    end

    def protocol_name
      "roundrobin"
    end

    def user_data
      nil
    end

    # Assign the topic partitions to the group members.
    #
    # @param members [Hash<String, Protocol::JoinGroupResponse::Metadata>] a hash
    #   mapping member ids to metadata
    # @param topics [Array<String>] topics
    # @return [Hash<String, Protocol::MemberAssignment>] a hash mapping member
    #   ids to assignments.
    def assign(members:, topics:)
      group_assignment = {}

      members.each_key do |member_id|
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

      partitions_per_member = topic_partitions.group_by.with_index do |_, index|
        index % members.count
      end.values

      members.keys.zip(partitions_per_member).each do |member_id, member_partitions|
        unless member_partitions.nil?
          member_partitions.each do |topic, partition|
            group_assignment[member_id].assign(topic, [partition])
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
