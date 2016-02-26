require "kafka/consistent_hash_ring_assignment_strategy"

describe Kafka::ConsistentHashRingAssignmentStrategy, "#assign" do
  let(:cluster) { double(:cluster) }
  let(:strategy) { described_class.new(cluster: cluster) }
  let(:topics) { ["greetings"] }
  let(:num_partitions) { 100 }
  let(:members) { ["a", "b", "c", "d"] }

  before do
    allow(cluster).to receive(:partitions_for).with("greetings") {
      num_partitions.times.map {|i| double(partition_id: i) }
    }
  end

  it "assigns all partitions" do
    assignments = strategy.assign(members: members, topics: topics)

    expect(
      assignments.values.map(&:partition_count).inject(0, &:+)
    ).to eq num_partitions
  end

  it "assigns partitions to all members" do
    assignments = strategy.assign(members: members, topics: topics)

    assignments.each do |member_id, assignment|
      expect(assignment.partition_count).to be >= 15
    end
  end

  context "when a member is removed from the group" do
    it "lets the other members keep their partitions" do
      first_assignments = strategy.assign(members: members, topics: topics)

      members.delete("b")
      second_assignments = strategy.assign(members: members, topics: topics)

      members.each do |member_id|
        fst = first_assignments.fetch(member_id)
        snd = second_assignments.fetch(member_id)

        fst.topics.each do |topic, partitions|
          expect(snd.topics.fetch(topic)).to include(*partitions)
        end
      end
    end
  end
end
