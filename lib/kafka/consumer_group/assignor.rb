# frozen_string_literal: true

require "kafka/protocol/member_assignment"

module Kafka
  class ConsumerGroup

    # A consumer group partition assignor
    class Assignor
      Strategy = Struct.new(:assign, :user_data)
      Partition = Struct.new(:topic, :partition_id)

      # @param name [String]
      # @param user_data [Proc, nil] a proc that returns user data as a string
      # @yield [cluster:, members:, partitions:]
      # @yieldparam cluster [Kafka::Cluster]
      # @yieldparam members [Hash<String, Kafka::Protocol::JoinGroupResponse::Metadata>]
      #   a hash mapping member ids to metadata
      # @yieldparam partitions [Array<Kafka::ConsumerGroup::Assignor::Partition>]
      #   a list of partitions the consumer group processes
      # @yieldreturn [Hash<String, Array<Kafka::ConsumerGroup::Assignor::Partition>]
      #   a hash mapping member ids to partitions.
      def self.register_strategy(name, user_data: nil, &block)
        if strategies[name.to_s]
          raise ArgumentError, "The strategy '#{name}' is already registered."
        end

        unless block_given?
          raise ArgumentError, "The block must be given."
        end

        strategies[name.to_s] = Strategy.new(block, user_data)
      end

      def self.strategies
        @strategies ||= {}
      end

      attr_reader :protocol_name

      # @param cluster [Kafka::Cluster]
      # @param strategy [String, Symbol, Object]  a string, a symbol, or an object that implements
      #   #user_data and #assign.
      def initialize(cluster:, strategy:)
        @cluster = cluster
        @protocol_name = strategy.to_s
        @strategy = self.class.strategies.fetch(@protocol_name) do
          raise ArgumentError, "The strategy '#{@protocol_name}' is not registered."
        end
      end

      def user_data
        @strategy.user_data.call if @strategy.user_data
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

        @strategy.assign.call(cluster: @cluster, members: members, partitions: topic_partitions).each do |member_id, partitions|
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
