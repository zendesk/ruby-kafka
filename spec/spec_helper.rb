$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "kafka"
require "dotenv"

Dotenv.load

KAFKA_HOST = ENV.fetch("KAFKA_HOST")
KAFKA_PORT = ENV.fetch("KAFKA_PORT").to_i
