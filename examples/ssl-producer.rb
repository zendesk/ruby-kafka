# frozen_string_literal: true

# Reads lines from STDIN, writing them to Kafka.

$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"

logger = Logger.new($stderr)
brokers = ENV.fetch("KAFKA_BROKERS")

# Make sure to create this topic in your Kafka cluster or configure the
# cluster to auto-create topics.
topic = "page-visits"

ssl_context = OpenSSL::SSL::SSLContext.new
ssl_context.set_params(
  cert: OpenSSL::X509::Certificate.new(ENV.fetch("KAFKA_CLIENT_CERT")),
  key: OpenSSL::PKey::RSA.new(ENV.fetch("KAFKA_CLIENT_CERT_KEY")),
)

kafka = Kafka.new(
  seed_brokers: brokers,
  client_id: "ssl-producer",
  logger: logger,
  ssl: true,
  ssl_context: ssl_context,
)

producer = kafka.producer

begin
  $stdin.each_with_index do |line, index|
    producer.produce(line, topic: topic)

    # Send messages for every 10 lines.
    producer.deliver_messages if index % 10 == 0
  end
ensure
  # Make sure to send any remaining messages.
  producer.deliver_messages

  producer.shutdown
end
