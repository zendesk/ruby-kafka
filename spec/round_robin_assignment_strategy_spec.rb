# frozen_string_literal: true

describe Kafka::RoundRobinAssignmentStrategy do
  it "assigns all partitions" do
    cluster = double(:cluster)
    strategy = described_class.new(cluster: cluster)

    members = (0...10).map {|i| "member#{i}" }
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

    members = (0...10).map {|i| "member#{i}" }
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
end
