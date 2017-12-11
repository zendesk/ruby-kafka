class CustomAssignmentStrategy
  def assign(members:, topics:)
    group_assignment = {}

    members.keys.each do |member_id|
      group_assignment[member_id] = Kafka::Protocol::MemberAssignment.new
    end

    topics.each do |topic, partitions|
      partitions = partitions.sort
      members.each do |member_id, metadata|
        if metadata == '0'
          group_assignment[member_id].assign(
            topic,
            partitions[0..(partitions.length / 2 - 1)]
          )
        else
          group_assignment[member_id].assign(
            topic,
            partitions[(partitions.length / 2)..(partitions.length - 1)]
          )
        end
      end
    end
    group_assignment
  end
end

describe "Group Protocol", functional: true do
  let(:num_partitions) { 10 }
  let!(:topic) { create_random_topic(num_partitions: num_partitions) }
  let(:offset_retention_time) { 30 }

  example "use custom group protocol" do
    messages_first = (1..100).to_a
    messages_second = (101..200).to_a

    Thread.new do
      kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test")
      producer = kafka.producer

      messages_first.each do |i|
        producer.produce(i.to_s, topic: topic, partition: rand(0..4))
      end
      producer.deliver_messages

      messages_second.each do |i|
        producer.produce(i.to_s, topic: topic, partition: rand(5..9))
      end
      producer.deliver_messages
    end

    group_id = "test#{rand(1000)}"

    threads = (0..1).map do |thread_id|
      t = Thread.new do
        sleep 1
        received_messages = []

        kafka = Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger)
        consumer = kafka.consumer(
          group_id: group_id,
          group_protocols: [
            name: 'custom_protocol',
            metadata: thread_id.to_s,
            assignment_strategy: CustomAssignmentStrategy
          ],
          offset_retention_time: offset_retention_time
        )
        consumer.subscribe(topic)

        consumer.each_message do |message|
          received_messages << Integer(message.value)
          consumer.stop if received_messages.length == 100
        end

        received_messages
      end

      t.abort_on_exception = true

      t
    end

    sleep 3
    expect(threads.first.value.sort).to match_array messages_first
    expect(threads.last.value.sort).to match_array messages_second
  end
end
