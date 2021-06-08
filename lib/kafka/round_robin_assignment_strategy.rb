module Kafka

  # A round robin assignment strategy inpired on the
  # original java client round robin assignor. It's capable
  # of handling identical as well as different topic subscriptions
  # accross the same consumer group.
  class RoundRobinAssignmentStrategy
    def protocol_name
      "roundrobin"
    end

    # Assign the topic partitions to the group members.
    #
    # @param cluster [Kafka::Cluster]
    # @param members [Hash<String, Kafka::Protocol::JoinGroupResponse::Metadata>] a hash
    #   mapping member ids to metadata
    # @param partitions [Array<Kafka::ConsumerGroup::Assignor::Partition>] a list of
    #   partitions the consumer group processes
    # @return [Hash<String, Array<Kafka::ConsumerGroup::Assignor::Partition>] a hash
    #   mapping member ids to partitions.
    def call(cluster:, members:, partitions:)
      partitions_per_member = Hash.new {|h, k| h[k] = [] }
      relevant_partitions = valid_sorted_partitions(members, partitions)
      members_ids = members.keys
      iterator = (0...members.size).cycle
      idx = iterator.next

      relevant_partitions.each do |partition|
        topic = partition.topic

        while !members[members_ids[idx]].topics.include?(topic)
          idx = iterator.next
        end

        partitions_per_member[members_ids[idx]] << partition
        idx = iterator.next
      end

      partitions_per_member
    end

    def valid_sorted_partitions(members, partitions)
      subscribed_topics = members.map do |id, metadata|
        metadata && metadata.topics
      end.flatten.compact

      partitions
        .select { |partition| subscribed_topics.include?(partition.topic) }
        .sort_by { |partition| partition.topic }
    end
  end
end
