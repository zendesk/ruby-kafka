$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "kafka"
require "dotenv"
require "logger"

require "test_cluster"

Dotenv.load

LOG = ENV.key?("LOG_TO_STDERR") ? $stderr : StringIO.new

KAFKA_TOPIC = "test-messages"

KAFKA_CLUSTER = TestCluster.new
KAFKA_CLUSTER.start
KAFKA_CLUSTER.create_topic(KAFKA_TOPIC, num_partitions: 3, num_replicas: 2)

KAFKA_BROKERS = KAFKA_CLUSTER.kafka_hosts

host, port = KAFKA_BROKERS.first.split(":", 2)

KAFKA_HOST = host
KAFKA_PORT = port.to_i

at_exit {
  KAFKA_CLUSTER.stop
}
