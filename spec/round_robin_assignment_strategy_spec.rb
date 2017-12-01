describe Kafka::RoundRobinAssignmentStrategy do
  it "assigns all partitions" do
    cluster = double(:cluster)
    strategy = described_class.new(cluster: cluster)

    members = Hash[*(0...10).map {|i| ["member#{i}", Kafka::Protocol::ConsumerGroupProtocol.new(:topics => ["greetings"])]}.flatten]
    partitions = (0...30).map {|i| double(:"partition#{i}", partition_id: i) }

    allow(cluster).to receive(:partitions_for) { partitions }

    assignments = strategy.assign(members: members)

    partitions.each do |partition|
      member = assignments.values.find {|assignment|
        assignment.topics.find {|topic, partitions|
          partitions.include?(partition.partition_id)
        }
      }

      expect(member).to_not be_nil
    end
  end

  it "assigns by desired topic" do
    cluster = double(:cluster)
    strategy = described_class.new(cluster: cluster)

    members = Hash[*(0...10).map {|i| ["member#{i}", Kafka::Protocol::ConsumerGroupProtocol.new(:topics => ["topic#{i % 2}"])] }.flatten]
    partitions = (0...30).map {|i| double(:"partition#{i}", partition_id: i) }

    allow(cluster).to receive(:partitions_for) { partitions }

    assignments = strategy.assign(members: members)

    assignments.each do |member, assignment|
      i_value = member[6].to_i
      assignment.topics.each_key do |topic_name|
        expect(i_value % 2).to eq(topic_name[5].to_i), "expected #{member} to not have #{topic_name}"
      end
    end
  end
end
