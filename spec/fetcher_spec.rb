require "kafka/fetcher"
require "fake_broker"

describe Kafka::Fetcher do
  it "fetches messages from the broker" do
    broker = FakeBroker.new
    commands = Queue.new
    output = Queue.new
    logger = Logger.new(StringIO.new)

    fetcher = Kafka::Fetcher.new(
      broker: broker,
      commands: commands,
      output: output,
      logger: logger,
    )

    broker.write("hello", topic: "topic1", partition: 0)
    broker.write("world", topic: "topic1", partition: 1)

    assignments = {
      "topic1" => [0, 1],
    }

    commands << [:assign, { assignments: assignments }]
    commands << [:fetch, {}]
    commands << [:shutdown, {}]

    fetcher.run

    expect(output.size).to eq 1

    batches = output.deq

    expect(batches.map(&:topic)).to eq ["topic1", "topic1"]
    expect(batches.map(&:partition)).to eq [0, 1]
    expect(batches.flat_map(&:messages).map(&:value)).to eq ["hello", "world"]
  end
end
