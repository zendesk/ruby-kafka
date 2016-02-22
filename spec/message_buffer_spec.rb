describe Kafka::MessageBuffer do
  describe "#size" do
    let(:buffer) { Kafka::MessageBuffer.new }

    it "returns the number of messages in the buffer" do
      buffer.concat(["a", "b", "c"], topic: "bar", partition: 3)
      buffer.concat(["a", "b", "c"], topic: "bar", partition: 1)

      expect(buffer.size).to eq 6
    end

    it "keeps track of how many messages have been cleared" do
      buffer.concat(["a", "b", "c"], topic: "bar", partition: 3)
      buffer.concat(["a", "b", "c"], topic: "bar", partition: 1)
      buffer.clear_messages(topic: "bar", partition: 3)

      expect(buffer.size).to eq 3
    end

    it "buffers messages quickly", performance: true do
      num_topics = 20
      num_partitions = 20
      num_messages = 10_000

      (1...num_messages).each do |i|
        topic = num_topics % i
        partition = num_partitions % i

        buffer.write("hello", topic: topic, partition: partition)
      end

      expect { buffer.size }.to perform_at_least(10000).ips
    end
  end
end
