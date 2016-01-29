describe Kafka::Producer do
  let(:logger) { Logger.new(LOG) }
  let(:broker1) { FakeBroker.new }
  let(:broker2) { FakeBroker.new }
  let(:broker_pool) { double(:broker_pool) }

  let(:producer) {
    Kafka::Producer.new(
      broker_pool: broker_pool,
      logger: logger,
      max_retries: 2,
      retry_backoff: 0,
    )
  }

  before do
    allow(broker_pool).to receive(:mark_as_stale!)

    allow(broker_pool).to receive(:get_leader_id).with("greetings", 0) { 1 }
    allow(broker_pool).to receive(:get_leader_id).with("greetings", 1) { 2 }

    allow(broker_pool).to receive(:get_broker).with(1) { broker1 }
    allow(broker_pool).to receive(:get_broker).with(2) { broker2 }
  end

  describe "#produce" do
    before do
      allow(broker_pool).to receive(:partitions_for).with("greetings") { [0, 1, 2, 3] }
    end

    it "writes the message to the buffer" do
      partition = producer.produce("hello", key: "greeting1", topic: "greetings")

      expect(partition).to eq 3
    end

    it "allows explicitly setting the partition" do
      partition = producer.produce("hello", key: "greeting1", topic: "greetings", partition: 1)

      expect(partition).to eq 1
    end

    it "allows implicitly setting the partition using a partition key" do
      partition = producer.produce("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(partition).to eq 0
    end
  end

  describe "#send_messages" do
    class FakeBroker
      def initialize
        @messages = {}
        @partition_errors = {}
      end

      def messages
        messages = []

        @messages.each do |topic, messages_for_topic|
          messages_for_topic.each do |partition, messages_for_partition|
            messages_for_partition.each do |message|
              messages << message.value
            end
          end
        end

        messages
      end

      def produce(messages_for_topics:, required_acks:, timeout:)
        messages_for_topics.each do |topic, messages_for_topic|
          messages_for_topic.each do |partition, messages|
            @messages[topic] ||= {}
            @messages[topic][partition] ||= []
            @messages[topic][partition].concat(messages)
          end
        end

        topics = messages_for_topics.map {|topic, messages_for_topic|
          Kafka::Protocol::ProduceResponse::TopicInfo.new(
            topic: topic,
            partitions: messages_for_topic.map {|partition, messages|
              Kafka::Protocol::ProduceResponse::PartitionInfo.new(
                partition: partition,
                error_code: error_code_for_partition(topic, partition),
                offset: messages.size,
              )
            }
          )
        }

        if required_acks != 0
          Kafka::Protocol::ProduceResponse.new(topics: topics)
        else
          nil
        end
      end

      def mark_partition_with_error(topic:, partition:, error_code:)
        @partition_errors[topic] ||= Hash.new { 0 }
        @partition_errors[topic][partition] = error_code
      end

      private

      def error_code_for_partition(topic, partition)
        @partition_errors[topic] ||= Hash.new { 0 }
        @partition_errors[topic][partition]
      end
    end

    it "sends messages to the leader of the partition being written to" do
      producer.produce("hello1", key: "greeting1", topic: "greetings", partition: 0)
      producer.produce("hello2", key: "greeting2", topic: "greetings", partition: 1)

      producer.send_messages

      expect(broker1.messages).to eq ["hello1"]
      expect(broker2.messages).to eq ["hello2"]
    end

    it "handles when a partition temporarily doesn't have a leader" do
      broker1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 5)

      producer.produce("hello1", topic: "greetings", partition: 0)

      expect { producer.send_messages }.to raise_error(Kafka::FailedToSendMessages)

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      broker1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 0)

      producer.send_messages

      expect(producer.buffer_size).to eq 0
    end

    it "clears the buffer after sending messages if no acknowledgements are required" do
      producer = Kafka::Producer.new(
        broker_pool: broker_pool,
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

    it "raises BufferOverflow if the max buffer size is exceeded" do
      producer = Kafka::Producer.new(
        broker_pool: broker_pool,
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
  end
end
