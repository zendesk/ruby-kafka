describe Kafka::Producer do
  let(:logger) { Logger.new(LOG) }
  let(:connection1) { double(:connection1) }
  let(:connection2) { double(:connection2) }
  let(:cluster) { double(:cluster) }

  let(:producer) {
    Kafka::Producer.new(
      cluster: cluster,
      logger: logger,
      max_retries: 2,
      retry_backoff: 0,
    )
  }

  before do
    allow(cluster).to receive(:mark_as_stale!)
    allow(cluster).to receive(:refresh_metadata_if_necessary!)
    allow(cluster).to receive(:add_target_topics)

    allow(cluster).to receive(:get_leader).with("greetings", 0) { connection1 }
    allow(cluster).to receive(:get_leader).with("greetings", 1) { connection2 }

    allow(connection1).to receive(:send_request)
  end

  describe "#produce" do
    before do
      allow(cluster).to receive(:partitions_for).with("greetings") { [0, 1, 2, 3] }
    end

    it "writes the message to the buffer" do
      producer.produce("hello", key: "greeting1", topic: "greetings")

      expect(producer.buffer_size).to eq 1
    end

    it "allows explicitly setting the partition" do
      producer.produce("hello", key: "greeting1", topic: "greetings", partition: 1)

      expect(producer.buffer_size).to eq 1
    end

    it "allows implicitly setting the partition using a partition key" do
      producer.produce("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(producer.buffer_size).to eq 1
    end

    it "raises BufferOverflow if the max buffer size is exceeded" do
      producer = Kafka::Producer.new(
        cluster: cluster,
        logger: logger,
        max_buffer_size: 2, # <-- this is the important bit.
      )

      producer.produce("hello1", topic: "greetings", partition: 0)
      producer.produce("hello1", topic: "greetings", partition: 0)

      expect {
        producer.produce("hello1", topic: "greetings", partition: 0)
      }.to raise_error(Kafka::BufferOverflow)

      expect(producer.buffer_size).to eq 2
    end

    it "works even when Kafka is unavailable" do
      allow(connection1).to receive(:send_request).and_raise(Kafka::Error)
      allow(connection2).to receive(:send_request).and_raise(Kafka::Error)

      producer.produce("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(producer.buffer_size).to eq 1

      # Only when we try to send the messages to Kafka do we experience the issue.
      expect {
        producer.send_messages
      }.to raise_exception(Kafka::Error)
    end
  end

  describe "#send_messages" do
    it "sends messages to the leader of the partition being written to" do
      producer.produce("hello1", key: "greeting1", topic: "greetings", partition: 0)
      producer.produce("hello2", key: "greeting2", topic: "greetings", partition: 1)

      producer.send_messages

      expect(connection1.messages).to eq ["hello1"]
      expect(connection2.messages).to eq ["hello2"]
    end

    it "handles when a partition temporarily doesn't have a leader" do
      connection1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 5)

      producer.produce("hello1", topic: "greetings", partition: 0)

      expect { producer.send_messages }.to raise_error(Kafka::FailedToSendMessages)

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      connection1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 0)

      producer.send_messages

      expect(producer.buffer_size).to eq 0
    end

    it "handles when there's a connection error when fetching topic metadata" do
      allow(cluster).to receive(:get_leader).and_raise(Kafka::ConnectionError)

      producer.produce("hello1", topic: "greetings", partition: 0)

      expect { producer.send_messages }.to raise_error(Kafka::FailedToSendMessages)

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      allow(cluster).to receive(:get_leader) { connection1 }

      producer.send_messages

      expect(producer.buffer_size).to eq 0
    end

    it "clears the buffer after sending messages if no acknowledgements are required" do
      producer = Kafka::Producer.new(
        cluster: cluster,
        logger: logger,
        required_acks: 0, # <-- this is the important bit.
        max_retries: 2,
        retry_backoff: 0,
      )

      producer.produce("hello1", topic: "greetings", partition: 0)
      producer.send_messages

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 0
    end
  end
end
