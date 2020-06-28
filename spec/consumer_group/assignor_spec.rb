# frozen_string_literal: true

describe Kafka::ConsumerGroup::Assignor do
  let(:cluster) { double(:cluster) }
  let(:assignor) { described_class.new(cluster: cluster, strategy: strategy) }
  let(:strategy) do
    klass = Class.new do
      def protocol_name
        "test"
      end

      def user_data
        nil
      end

      def assign(members:, partitions:)
        assignment = {}
        partition_count_per_member = (partitions.count.to_f / members.count).ceil
        partitions.each_slice(partition_count_per_member).with_index do |chunk, index|
          assignment[members.keys[index]] = chunk
        end

        assignment
      end
    end
    klass.new
  end

  it "assigns all partitions" do
    members = Hash[(0...10).map {|i| ["member#{i}", nil] }]
    topics = ["greetings"]
    partitions = (0...30).map {|i| double(:"partition#{i}", partition_id: i) }

    allow(cluster).to receive(:partitions_for) { partitions }

    assignments = assignor.assign(members: members, topics: topics)

    partitions.each do |partition|
      member = assignments.values.find {|assignment|
        assignment.topics.find {|topic, partitions|
          partitions.include?(partition.partition_id)
        }
      }

      expect(member).to_not be_nil
    end
  end
end
