describe Kafka::Partitioner, "#partition_for_key" do
  let(:partitioner) { Kafka::Partitioner.new([0, 1, 2]) }

  it "deterministically returns a partition number for a given key and number of partitions" do
    partition = partitioner.partition_for_key("yolo")
    expect(partition).to eq 0
  end

  it "randomly picks a partition if the key is nil" do
    partitions = 30.times.map { partitioner.partition_for_key(nil) }
    expect(partitions.uniq).to contain_exactly(0, 1, 2)
  end
end
