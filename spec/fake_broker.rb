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
          messages << message
        end
      end
    end

    messages
  end

  def produce(messages_for_topics:, required_acks:, timeout:)
    messages_for_topics.each do |topic, messages_for_topic|
      messages_for_topic.each do |partition, message_set|
        @messages[topic] ||= {}
        @messages[topic][partition] ||= []
        @messages[topic][partition].concat(message_set.messages)
      end
    end

    topics = messages_for_topics.map {|topic, messages_for_topic|
      Kafka::Protocol::ProduceResponse::TopicInfo.new(
        topic: topic,
        partitions: messages_for_topic.map {|partition, message_set|
          Kafka::Protocol::ProduceResponse::PartitionInfo.new(
            partition: partition,
            error_code: error_code_for_partition(topic, partition),
            offset: message_set.messages.size,
            timestamp: (Time.now.to_f*1000).to_i,
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
