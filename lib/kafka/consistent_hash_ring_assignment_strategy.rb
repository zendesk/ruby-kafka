require "zlib"
require "kafka/protocol/member_assignment"

module Kafka
  class ConsistentHashRingAssignmentStrategy
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

      replicas = 1000
      ring = {}

      members.each do |member_id|
        replicas.times do |i|
          key = hash(member_id + i.to_s)

          ring[key] = member_id
        end
      end

      topics.each do |topic|
        partitions = @cluster.partitions_for(topic).map(&:partition_id)
        partitions_for_member = {}

        partitions.each do |partition|
          key = hash(topic + partition.to_s)
          nearest_key = ring.keys.sort.find {|k| k >= key } || ring.keys.sort.first
          member_id = ring.fetch(nearest_key)

          partitions_for_member[member_id] ||= []
          partitions_for_member[member_id] << partition
        end

        members.each do |member_id|
          member_partitions = partitions_for_member.fetch(member_id, [])

          if member_partitions.any?
            group_assignment[member_id].assign(topic, member_partitions)
          end
        end
      end

      group_assignment
    end

    private

    def hash(key)
      Digest::SHA256.hexdigest(key).to_i(16)
    end
  end
end
