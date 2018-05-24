# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "active_support/notifications"
require "kafka"
require "dotenv"
require "logger"
require "rspec-benchmark"
require "colored"
require "securerandom"
require 'snappy'
require 'extlz4'

Dotenv.load

LOGGER = Logger.new(ENV.key?("LOG_TO_STDERR") ? $stderr : "test-#{Time.now.to_i}.log")
LOGGER.level = Logger.const_get(ENV.fetch("LOG_LEVEL", "INFO"))

KAFKA_BROKERS = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

# A unique id for the test run, used to namespace global resources.
RUN_ID = SecureRandom.hex(8)

class LogFormatter < Logger::Formatter
  def call(severity, time, progname, msg)
    str = "#{msg2str(msg)}\n"
    color = color_for(severity)

    Colored.colorize(str, foreground: color)
  end

  private

  def color_for(severity)
    case severity
    when "INFO"
      "cyan"
    when "WARN"
      "yellow"
    when "ERROR"
      "red"
    else
      nil
    end
  end
end

if ENV["LOG_COLORS"] == "true"
  LOGGER.formatter = LogFormatter.new
end

module SpecHelpers
  def generate_topic_name
    @@topic_number ||= 0
    @@topic_number += 1

    "#{RUN_ID}-topic-#{@@topic_number}"
  end

  def create_random_topic(*args)
    topic = generate_topic_name
    create_topic(topic, *args)
    topic
  end

  def create_topic(name, num_partitions: 1, num_replicas: 1)
    kafka.create_topic(name, num_partitions: num_partitions, replication_factor: num_replicas)
  end
end

module FunctionalSpecHelpers
  def self.included(base)
    base.class_eval do
      let(:logger) { LOGGER }
      let(:kafka_brokers) { KAFKA_BROKERS }
      let(:kafka) { Kafka.new(kafka_brokers, client_id: "test", logger: logger) }

      after { kafka.close rescue nil }
    end
  end
end

RSpec.configure do |config|
  config.filter_run_excluding functional: true, performance: true, fuzz: true
  config.include RSpec::Benchmark::Matchers
  config.include SpecHelpers
  config.include FunctionalSpecHelpers, functional: true
  config.include FunctionalSpecHelpers, fuzz: true
end

ActiveSupport::Notifications.subscribe(/.*\.kafka$/) do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  LOGGER.debug "Instrumentation event `#{event.name}`: #{event.payload.inspect}"
end
