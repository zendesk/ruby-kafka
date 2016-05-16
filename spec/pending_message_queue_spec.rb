describe Kafka::PendingMessageQueue do
  describe "#prune_oldest" do
    it "removes the oldest 20% of messages" do
      queue = Kafka::PendingMessageQueue.new

      messages = 1.upto(100).map {|i| message = double("message#{i}", bytesize: 100) }
      messages.each {|message| queue.write(message) }

      expect(queue.bytesize).to eq 10_000

      queue.prune_oldest

      expect(queue.bytesize).to eq 8_000
      expect(queue.each.first).to eq messages[20]
    end
  end
end
