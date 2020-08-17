# frozen_string_literal: true

require "kafka/protocol/member_assignment"

module Kafka
  class ConsumerGroup

    # A consumer group partition assignor
    class Assignor
      Partition = Struct.new(:topic, :partition_id)

      # @param cluster [Kafka::Cluster]
      # @param strategy [Object] an object that implements #protocol_type,
      #   #user_data, and #assign.
      def initialize(cluster:, strategy:)
        @cluster = cluster
        @strategy = strategy
      end

      def protocol_name
        @strategy.respond_to?(:protocol_name) ? @strategy.protocol_name : @strategy.class.to_s
      end

      def user_data
        @strategy.user_data if @strategy.respond_to?(:user_data)
      end

      # Assign the topic partitions to the group members.
      #
      # @param members [Hash<String, Kafka::Protocol::JoinGroupResponse::Metadata>] a hash
      #   mapping member ids to metadata.
      # @param topics [Array<String>] topics
      # @return [Hash<String, Kafka::Protocol::MemberAssignment>] a hash mapping member
      #   ids to assignments.
      def assign(members:, topics:)
        topic_partitions = topics.flat_map do |topic|
          begin
            partition_ids = @cluster.partitions_for(topic).map(&:partition_id)
          rescue UnknownTopicOrPartition
            raise UnknownTopicOrPartition, "unknown topic #{topic}"
          end
          partition_ids.map {|partition_id| Partition.new(topic, partition_id) }
        end

        group_assignment = {}

        members.each_key do |member_id|
          group_assignment[member_id] = Protocol::MemberAssignment.new
        end
        @strategy.call(cluster: @cluster, members: members, partitions: topic_partitions).each do |member_id, partitions|
          Array(partitions).each do |partition|
            group_assignment[member_id].assign(partition.topic, [partition.partition_id])
          end
        end

        group_assignment
      rescue Kafka::LeaderNotAvailable
        sleep 1
        retry
      end
    end
  end
end
