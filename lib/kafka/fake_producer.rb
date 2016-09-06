module Kafka
  class FakeProducer
    PARTITIONS_PER_TOPIC = 32

    def initialize
      @logs = Hash.new {|h, topic|
        h[topic] = Hash.new {|logs, partition|
          logs[partition] = []
        }
      }
    end

    def produce(value, key: nil, topic:, partition: nil, partition_key: nil)
      partition ||= Partitioner.partition_for_key(
        PARTITIONS_PER_TOPIC,
        partition_key || key,
      )

      log = @logs[topic][partition]

      message = FetchedMessage.new(
        value: value,
        key: key,
        topic: topic,
        partition: partition,
        offset: log.size,
      )

      log << message

      nil
    end

    def deliver_messages
    end

    def messages_in(topic, partition = nil)
      if partition
        @logs[topic][partition]
      else
        @logs[topic].inject([]) {|messages, (_, log)| messages + log }
      end
    end
  end
end
