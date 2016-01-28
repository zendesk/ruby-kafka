describe Kafka::MessageBuffer do
  describe "#size" do
    it "returns the number of messages in the buffer" do
      buffer = Kafka::MessageBuffer.new
      buffer.concat(["a", "b", "c"], topic: "foo", partition: 3)
      buffer.concat(["a", "b", "c"], topic: "bar", partition: 1)

      expect(buffer.size).to eq 6
    end
  end
end
