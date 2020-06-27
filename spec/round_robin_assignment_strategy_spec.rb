# frozen_string_literal: true

describe Kafka::RoundRobinAssignmentStrategy do
  let(:cluster) { double(:cluster) }
  let(:strategy) { described_class.new(cluster: cluster) }

  it "assigns all partitions" do
    members = Hash[(0...10).map {|i| ["member#{i}", nil] }]
    topics = ["greetings"]
    partitions = (0...30).map {|i| double(:"partition#{i}", partition_id: i) }

    allow(cluster).to receive(:partitions_for) { partitions }

    assignments = strategy.assign(members: members, topics: topics)

    partitions.each do |partition|
      member = assignments.values.find {|assignment|
        assignment.topics.find {|topic, partitions|
          partitions.include?(partition.partition_id)
        }
      }

      expect(member).to_not be_nil
    end
  end

  it "spreads all partitions between members" do
    cluster = double(:cluster)
    strategy = described_class.new(cluster: cluster)

    members = Hash[(0...10).map {|i| ["member#{i}", nil] }]
    topics = ["topic1", "topic2"]
    partitions = (0...5).map {|i| double(:"partition#{i}", partition_id: i) }

    allow(cluster).to receive(:partitions_for) { partitions }

    assignments = strategy.assign(members: members, topics: topics)

    partitions.each do |partition|
      member = assignments.values.find {|assignment|
        assignment.topics.find {|topic, partitions|
          partitions.include?(partition.partition_id)
        }
      }

      expect(member).to_not be_nil
    end

    num_partitions_assigned = assignments.values.map do |assignment|
      assignment.topics.values.map(&:count).inject(:+)
    end

    expect(num_partitions_assigned).to all eq(1)
  end

  [
    {
      name: "uneven topics",
      topics: { "topic1" => [0], "topic2" => (0..50).to_a },
      members: { "member1" => nil, "member2" => nil },
    },
    {
      name: "only one partition",
      topics: { "topic1" => [0] },
      members: { "member1" => nil, "member2" => nil },
    },
    {
      name: "lots of partitions",
      topics: { "topic1" => (0..100).to_a },
      members: { "member1" => nil },
    },
    {
      name: "lots of members",
      topics: { "topic1" => (0..10).to_a, "topic2" => (0..10).to_a },
      members: Hash[(0..50).map { |i| ["member#{i}", nil] }]
    },
    {
      name: "odd number of partitions",
      topics: { "topic1" => (0..14).to_a },
      members: { "member1" => nil, "member2" => nil },
    },
    {
      name: "five topics, 10 partitions, 3 consumers",
      topics: { "topic1" => [0, 1], "topic2" => [0, 1], "topic3" => [0, 1], "topic4" => [0, 1], "topic5" => [0, 1] },
      members: { "member1" => nil, "member2" => nil, "member3" => nil },
    }
  ].each do |name:, topics:, members:|
      it name do
        allow(cluster).to receive(:partitions_for) do |topic|
          topics.fetch(topic).map do |partition_id|
            double(:"partition#{partition_id}", partition_id: partition_id)
          end
        end

        assignments = strategy.assign(members: members, topics: topics.keys)

        expect_all_partitions_assigned(topics, assignments)
        expect_even_assignments(topics, assignments)
      end
    end

  def expect_all_partitions_assigned(topics, assignments)
    topics.each do |topic, partitions|
      partitions.each do |partition|
        assigned = assignments.values.find do |assignment|
          assignment.topics.fetch(topic, []).include?(partition)
        end
        expect(assigned).to_not be_nil
      end
    end
  end

  def expect_even_assignments(topics, assignments)
    num_partitions = topics.values.flatten.count
    assignments.values.each do |assignment|
      num_assigned = assignment.topics.values.flatten.count
      expect(num_assigned).to be_within(1).of(num_partitions.to_f / assignments.count)
    end
  end
end
