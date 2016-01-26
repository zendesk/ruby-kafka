describe Kafka::Producer do
  let(:logger) { Logger.new(StringIO.new) }
  let(:producer) { Kafka::Producer.new(broker_pool: broker_pool, logger: logger) }
  let(:broker_pool) { double(:broker_pool) }

  describe "#write" do
    before do
      allow(broker_pool).to receive(:partitions_for).with("greetings") { [0, 1, 2, 3] }
    end

    it "writes the message to the buffer" do
      partition = producer.write("hello", key: "greeting1", topic: "greetings")

      expect(partition).to eq 3
    end

    it "allows explicitly setting the partition" do
      partition = producer.write("hello", key: "greeting1", topic: "greetings", partition: 1)

      expect(partition).to eq 1
    end

    it "allows implicitly setting the partition using a partition key" do
      partition = producer.write("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(partition).to eq 0
    end
  end

  describe "#flush" do
    class FakeBroker
      def initialize
        @messages = {}
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
                error_code: 0,
                offset: messages.size,
              )
            }
          )
        }

        Kafka::Protocol::ProduceResponse.new(topics: topics)
      end
    end

    it "sends messages to the leader of the partition being written to" do
      broker1 = FakeBroker.new
      broker2 = FakeBroker.new

      allow(broker_pool).to receive(:get_leader).with("greetings", 0) { broker1 }
      allow(broker_pool).to receive(:get_leader).with("greetings", 1) { broker2 }

      partition = producer.write("hello1", key: "greeting1", topic: "greetings", partition: 0)
      partition = producer.write("hello2", key: "greeting2", topic: "greetings", partition: 1)

      producer.flush

      expect(broker1.messages).to eq ["hello1"]
      expect(broker2.messages).to eq ["hello2"]
    end
  end
end
