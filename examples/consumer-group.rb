# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"

logger = Logger.new(STDOUT)
brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

# Make sure to create this topic in your Kafka cluster or configure the
# cluster to auto-create topics.
topic = "text"

kafka = Kafka.new(
  seed_brokers: brokers,
  client_id: "test",
  socket_timeout: 20,
  logger: logger,
)

consumer = kafka.consumer(group_id: "test")
consumer.subscribe(topic)

trap("TERM") { consumer.stop }
trap("INT") { consumer.stop }

begin
  consumer.each_message do |message|
  end
rescue Kafka::ProcessingError => e
  warn "Got #{e.cause}"
  consumer.pause(e.topic, e.partition, timeout: 20)

  retry
end
