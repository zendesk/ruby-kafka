$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "kafka"

logger = Logger.new(STDOUT)
logger.level = Logger::INFO
logger.formatter = ->(_, _, _, msg) { msg }

STDOUT.sync = true

$kafka = Kafka.new(
  logger: logger,
  seed_brokers: ENV.fetch("HEROKU_KAFKA_URL"),
  ssl_ca_cert: ENV.fetch("HEROKU_KAFKA_TRUSTED_CERT"),
  ssl_client_cert: ENV.fetch("HEROKU_KAFKA_CLIENT_CERT"),
  ssl_client_cert_key: ENV.fetch("HEROKU_KAFKA_CLIENT_CERT_KEY"),
)
