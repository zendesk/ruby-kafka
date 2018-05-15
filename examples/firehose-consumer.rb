# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"
require "dotenv"

Dotenv.load

KAFKA_CLIENT_CERT = ENV.fetch("KAFKA_CLIENT_CERT")
KAFKA_CLIENT_CERT_KEY = ENV.fetch("KAFKA_CLIENT_CERT_KEY")
KAFKA_SERVER_CERT = ENV.fetch("KAFKA_SERVER_CERT")
KAFKA_URL = ENV.fetch("KAFKA_URL")
KAFKA_BROKERS = KAFKA_URL
KAFKA_TOPIC = "test-messages"

NUM_THREADS = 4

queue = Queue.new

threads = NUM_THREADS.times.map do |worker_id|
  Thread.new do
    logger = Logger.new($stderr)
    logger.level = Logger::INFO

    logger.formatter = proc {|severity, datetime, progname, msg|
      "[#{worker_id}] #{severity.ljust(5)} -- #{msg}\n"
    }

    kafka = Kafka.new(
      seed_brokers: KAFKA_BROKERS,
      logger: logger,
      connect_timeout: 30,
      socket_timeout: 30,
      ssl_client_cert: KAFKA_CLIENT_CERT,
      ssl_client_cert_key: KAFKA_CLIENT_CERT_KEY,
      ssl_ca_cert: KAFKA_SERVER_CERT,
    )

    consumer = kafka.consumer(group_id: "firehose")
    consumer.subscribe(KAFKA_TOPIC)

    i = 0
    consumer.each_message do |message|
      i += 1

      if i % 1000 == 0
        queue << i
        i = 0
      end

      sleep 0.01
    end
  end
end

threads.each {|t| t.abort_on_exception = true }

received_messages = 0

loop do
  received_messages += queue.pop
  puts "===> Received #{received_messages} messages"
end
