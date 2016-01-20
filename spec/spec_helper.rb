$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "kafka"
require "dotenv"
require "logger"

Dotenv.load

KAFKA_BROKERS = ENV.fetch("KAFKA_BROKERS").split(",").map(&:strip)

host, port = KAFKA_BROKERS.first.split(":", 2)

KAFKA_HOST = host
KAFKA_PORT = port.to_i
