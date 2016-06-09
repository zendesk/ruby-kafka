$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))
$LOAD_PATH.unshift(File.expand_path("../../spec", __FILE__))

require "kafka"
require "ruby-prof"
require "dotenv"
require "test_cluster"

Dotenv.load

# Number of times do iterate.
N = 10_000

KAFKA_CLUSTER = TestCluster.new
KAFKA_CLUSTER.start

logger = Logger.new(nil)

kafka = Kafka.new(
  seed_brokers: KAFKA_CLUSTER.kafka_hosts,
  client_id: "test",
  logger: logger,
)

producer = kafka.producer(
  max_buffer_size: 100_000,
)

RubyProf.start

N.times do
  producer.produce("hello", topic: "greetings")
end

result = RubyProf.stop
printer = RubyProf::FlatPrinter.new(result)
printer.print(STDOUT)

KAFKA_CLUSTER.stop
