# frozen_string_literal: true

describe Kafka::Partitioner, "#call" do
  let(:message) { double(:message, key: nil, partition_key: "yolo") }

  describe "default partitioner" do
    let(:partitioner) { Kafka::Partitioner.new }

    it "deterministically returns a partition number for a partition key and partition count" do
      partition = partitioner.call(3, message)
      expect(partition).to eq 0
    end

    it "falls back to the message key if no partition key is available" do
      allow(message).to receive(:partition_key) { nil }
      allow(message).to receive(:key) { "hey" }

      partition = partitioner.call(3, message)

      expect(partition).to eq 2
    end

    it "randomly picks a partition if the key is nil" do
      allow(message).to receive(:key) { nil }
      allow(message).to receive(:partition_key) { nil }

      partitions = 30.times.map { partitioner.call(3, message) }

      expect(partitions.uniq).to contain_exactly(0, 1, 2)
    end
  end

  describe "murmur2 partitioner" do
    let(:partitioner) { Kafka::Murmur2Partitioner.new }
    let(:message) { double(:message, key: nil, partition_key: "yolo") }

    it "deterministically returns a partition number for a partition key and partition count" do
      partition = partitioner.call(3, message)
      expect(partition).to eq 0
    end

    it "falls back to the message key if no partition key is available" do
      allow(message).to receive(:partition_key) { nil }
      allow(message).to receive(:key) { "hey" }

      partition = partitioner.call(3, message)

      expect(partition).to eq 1
    end

    it "randomly picks a partition if the key is nil" do
      allow(message).to receive(:key) { nil }
      allow(message).to receive(:partition_key) { nil }

      partitions = 30.times.map { partitioner.call(3, message) }

      expect(partitions.uniq).to contain_exactly(0, 1, 2)
    end

    it "picks a Java Kafka compatible partition" do
      partition_count = 100
      {
        # librdkafka test cases taken from tests/0048-partitioner.c
        "" => 0x106e08d9 % partition_count,
        "this is another string with more length to it perhaps" => 0x4f7703da % partition_count,
        "hejsan" => 0x5ec19395 % partition_count,
        # Java Kafka test cases taken from UtilsTest.java.
        # The Java tests check the result of murmur2 directly,
        # so have been ANDd with 0x7fffffff to work here
        "21" => (-973932308 & 0x7fffffff) % partition_count,
        "foobar" => (-790332482 & 0x7fffffff) % partition_count,
        "a-little-bit-long-string" => (-985981536 & 0x7fffffff) % partition_count,
        "a-little-bit-longer-string" => (-1486304829 & 0x7fffffff) % partition_count,
        "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8" => (-58897971 & 0x7fffffff) % partition_count
      }.each do |key, partition|
        allow(message).to receive(:partition_key) { key }
        expect(partitioner.call(partition_count, message)).to eq partition
      end
    end
  end
end
