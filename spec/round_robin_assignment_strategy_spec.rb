describe Kafka::RoundRobinAssignmentStrategy do
  it "assigns all partitions" do
    strategy = described_class.new

    members = (0...10).each_with_object({}) do |i, hash|
      hash["member#{i}"] = "metadata#{i}"
    end
    partition_ids = (0...30).to_a
    topics = { "greetings" => partition_ids }

    assignments = strategy.assign(members: members, topics: topics)

    partition_ids.each do |partition_id|
      member = assignments.values.find {|assignment|
        assignment.topics.find {|topic, partitions|
          partitions.include?(partition_id)
        }
      }

      expect(member).to_not be_nil
    end
  end
end
