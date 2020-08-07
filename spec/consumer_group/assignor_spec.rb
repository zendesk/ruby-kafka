# frozen_string_literal: true

describe Kafka::ConsumerGroup::Assignor do
  describe ".register_strategy" do
    context "when another strategy is already registered with the same name" do
      around do |example|
        described_class.register_strategy(:custom) { |**kwargs| [] }
        example.run
        described_class.strategies.delete("custom")
      end

      it do
        expect { described_class.register_strategy(:custom) { |**kwargs| [] } }.to raise_error(ArgumentError)
      end
    end
  end

  describe "#assign" do
    let(:cluster) { double(:cluster) }
    let(:assignor) { described_class.new(cluster: cluster, strategy: :custom) }

    let(:members) { Hash[(0...10).map {|i| ["member#{i}", nil] }] }
    let(:topics) { ["greetings"] }
    let(:partitions) { (0...30).map {|i| double(:"partition#{i}", partition_id: i) } }

    before do
      allow(cluster).to receive(:partitions_for) { partitions }

      described_class.register_strategy(:custom) do |cluster:, members:, partitions:|
        assignment = {}
        partition_count_per_member = (partitions.count.to_f / members.count).ceil
        partitions.each_slice(partition_count_per_member).with_index do |chunk, index|
          assignment[members.keys[index]] = chunk
        end

        assignment
      end
    end
    after do
      described_class.strategies.delete("custom")
    end

    it "assigns all partitions" do
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
end
