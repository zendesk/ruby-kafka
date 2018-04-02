describe "Consumer API", functional: true do
  let!(:topic) { create_random_topic(num_partitions: 2) }
  let(:consumer) { kafka.consumer(group_id: "consumer-name") }
  let(:v0p0) { "v0.p0" }
  let(:v1p0) { "v1.p0" }
  let(:v2p1) { "v2.p1" }

  before do
    kafka.deliver_message(v0p0, topic: topic, partition: 0)
    kafka.deliver_message(v1p0, topic: topic, partition: 0)
    kafka.deliver_message(v2p1, topic: topic, partition: 1)

    consumer.subscribe(topic)
  end

  def process_messages(automatically_mark_as_processed: false)
    Thread.abort_on_exception = true
    Thread.new do
      Thread.current[:processed_messages] = []

      consumer.each_message(
        automatically_mark_as_processed: automatically_mark_as_processed
      ) do |message|
        Thread.current[:processed_messages] << message.value

        if message.value == v0p0
          consumer.pause(message.topic, message.partition, timeout: 2)
        end
      end
    end
  end

  example "Pausing a partition with automatically_mark_as_processed=false" do
    runner_thread = process_messages(automatically_mark_as_processed: false)
    sleep 5
    runner_thread.exit

    expect(runner_thread[:processed_messages]).to include(v0p0, v2p1)
    expect(runner_thread[:processed_messages]).to_not include(v1p0)
  end

  example "Pausing a partition with automatically_mark_as_processed=true" do
    runner_thread = process_messages(automatically_mark_as_processed: true)
    sleep 5
    runner_thread.exit

    expect(runner_thread[:processed_messages]).to include(v0p0, v1p0, v2p1)
  end
end
