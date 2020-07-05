# frozen_string_literal: true

require "kafka/protocol/member_assignment"

module Kafka
  class ConsumerGroup

    # A consumer group partition assignor
    class Assignor
      Partition = Struct.new(:topic, :partition_id)

      def self.register_strategy(name, strategy_class = nil, &block)
        if strategy_classes[name.to_s]
          raise ArgumentError, "The strategy '#{name}' is already registered."
        end

        unless strategy_class.nil? ^ !block_given?
          raise "Either strategy class or block but not both must be specified."
        end

        if block_given?
          strategy_class = Class.new do
            define_method(:assign) do |cluster:, members:, partitions:|
              block.call(cluster: cluster, members: members, partitions: partitions)
            end
          end
        end

        strategy_classes[name.to_s] = strategy_class
      end

      def self.strategy_classes
        @strategy_classes ||= {}
      end

      attr_reader :protocol_name

      # @param cluster [Kafka::Cluster]
      # @param strategy [String, Symbol, Object]  a string, a symbol, or an object that implements
      #   #user_data and #assign.
      def initialize(cluster:, strategy:)
        @cluster = cluster

        case strategy
        when String, Symbol
          @protocol_name = strategy.to_s
          klass = self.class.strategy_classes.fetch(@protocol_name) do
            raise ArgumentError, "The strategy '#{@protocol_name}' is not registered."
          end
          @strategy = klass.new
        else
          @protocol_name, _ = self.class.strategy_classes.find {|name, klass| strategy.is_a?(klass) }
          if @protocol_name.nil?
            raise ArgumentError, "The strategy class '#{strategy.class}' is not registered."
          end
          @strategy = strategy
        end
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
        @strategy.assign(cluster: @cluster, members: members, partitions: topic_partitions).each do |member_id, partitions|
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
