describe Kafka::Partitioner, "#partition_for_key" do
  let(:partitioner) { Kafka::Partitioner }
  let(:message) { double(:message, key: nil, partition_key: "yolo") }

  it "deterministically returns a partition number for a partition key and partition count" do
    partition = partitioner.partition_for_key(3, message)
    expect(partition).to eq 0
  end

  it "falls back to the message key if no partition key is available" do
    allow(message).to receive(:partition_key) { nil }
    allow(message).to receive(:key) { "hey" }

    partition = partitioner.partition_for_key(3, message)

    expect(partition).to eq 2
  end

  it "randomly picks a partition if the key is nil" do
    allow(message).to receive(:key) { nil }
    allow(message).to receive(:partition_key) { nil }

    partitions = 30.times.map { partitioner.partition_for_key(3, message) }

    expect(partitions.uniq).to contain_exactly(0, 1, 2)
  end
end
