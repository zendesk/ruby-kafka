$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"

logger = Logger.new($stderr)
brokers = ENV.fetch("KAFKA_BROKERS").split(",")

# Make sure to create this topic in your Kafka cluster or configure the
# cluster to auto-create topics.
topic = "random-messages"

kafka = Kafka.new(
  seed_brokers: brokers,
  client_id: "simple-producer",
  logger: logger,
)

producer = kafka.get_producer

begin
  $stdin.each_with_index do |line, index|
    producer.produce(line, topic: topic)

    # Send messages for every 10 lines.
    producer.send_messages if index % 10 == 0
  end
ensure
  # Make sure to send any remaining messages.
  producer.send_messages

  producer.shutdown
end
