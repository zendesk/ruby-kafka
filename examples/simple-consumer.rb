# frozen_string_literal: true

# Consumes lines from a Kafka partition and writes them to STDOUT.
#
# You need to define the environment variable KAFKA_BROKERS for this
# to work, e.g.
#
#     export KAFKA_BROKERS=localhost:9092
#

$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"

# We don't want log output to clutter the console. Replace `StringIO.new`
# with e.g. `$stderr` if you want to see what's happening under the hood.
logger = Logger.new(StringIO.new)

brokers = ENV.fetch("KAFKA_BROKERS").split(",")

# Make sure to create this topic in your Kafka cluster or configure the
# cluster to auto-create topics.
topic = "text"

kafka = Kafka.new(
  seed_brokers: brokers,
  client_id: "simple-consumer",
  socket_timeout: 20,
  logger: logger,
)

kafka.each_message(topic: topic) do |message|
  puts message.value
end
