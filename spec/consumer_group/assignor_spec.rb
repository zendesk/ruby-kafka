# frozen_string_literal: true

describe Kafka::ConsumerGroup::Assignor do
  let(:cluster) { double(:cluster) }
  let(:assignor) { described_class.new(cluster: cluster, strategy: strategy) }

  describe "#protocol_name" do
    subject(:protocol_name) { assignor.protocol_name }

    context "when the strategy has the method" do
      let(:strategy) { double(protocol_name: "custom") }

      it "returns the return value" do
        expect(protocol_name).to eq "custom"
      end
    end

    context "when the strategy has the method" do
      let(:strategy) { Class.new.new }

      it "returns the return value" do
        expect(protocol_name).to match /\A#<Class:0x[0-9a-f]+>\z/
      end
    end
  end

  describe "#user_data" do
    subject(:user_data) { assignor.user_data }

    context "when the strategy has the method" do
      let(:strategy) { double(user_data: "user_data") }

      it "returns the return value" do
        expect(user_data).to eq "user_data"
      end
    end

    context "when the strategy has the method" do
      let(:strategy) { Class.new.new }

      it "returns the return value" do
        expect(user_data).to be_nil
      end
    end
  end

  describe "#assign" do
    let(:strategy) do
      klass = Class.new do
        def call(cluster:, members:, partitions:)
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
end
