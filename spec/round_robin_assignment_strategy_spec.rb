# frozen_string_literal: true

describe Kafka::RoundRobinAssignmentStrategy do
  let(:strategy) { described_class.new }

  it "assigns all partitions" do
    members = Hash[(0...10).map {|i| ["member#{i}", nil] }]
    partitions = (0...30).map {|i| double(:"partition#{i}", topic: "greetings", partition_id: i) }

    assignments = strategy.assign(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end
  end

  it "spreads all partitions between members" do
    members = Hash[(0...10).map {|i| ["member#{i}", nil] }]
    topics = ["topic1", "topic2"]
    partitions = topics.product((0...5).to_a).map {|topic, i|
      double(:"partition#{i}", topic: topic, partition_id: i)
    }

    assignments = strategy.assign(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end

    num_partitions_assigned = assignments.values.map do |assigned_partitions|
      assigned_partitions.count
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
        partitions = topics.flat_map {|topic, partition_ids|
          partition_ids.map {|i|
            double(:"partition#{i}", topic: topic, partition_id: i)
          }
        }

        assignments = strategy.assign(cluster: nil, members: members, partitions: partitions)

        expect_all_partitions_assigned(topics, assignments)
        expect_even_assignments(topics, assignments)
      end
    end

  def expect_all_partitions_assigned(topics, assignments)
    topics.each do |topic, partition_ids|
      partition_ids.each do |partition_id|
        assigned = assignments.values.find do |assigned_partitions|
          assigned_partitions.find {|assigned_partition|
            assigned_partition.topic == topic && assigned_partition.partition_id == partition_id
          }
        end
        expect(assigned).to_not be_nil
      end
    end
  end

  def expect_even_assignments(topics, assignments)
    num_partitions = topics.values.flatten.count
    assignments.values.each do |assigned_partition|
      num_assigned = assigned_partition.count
      expect(num_assigned).to be_within(1).of(num_partitions.to_f / assignments.count)
    end
  end
end
