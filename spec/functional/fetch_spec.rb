describe "Fetch API", functional: true do
  example "fetching from a non-existing topic when auto-create is enabled" do
    topic = "rand#{rand(1000)}"
    attempt = 1
    messages = nil

    begin
      messages = kafka.fetch_messages(
        topic: topic,
        partition: 0,
        offset: 0,
        max_wait_time: 0.1
      )
    rescue Kafka::LeaderNotAvailable, Kafka::NotLeaderForPartition
      if attempt < 10
        attempt += 1
        sleep 0.1
        retry
      else
        raise "timed out"
      end
    end

    expect(messages).to eq []
  end
end
