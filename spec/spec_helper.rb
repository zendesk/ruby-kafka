$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "active_support/notifications"
require "kafka"
require "dotenv"
require "logger"
require "rspec-benchmark"
require "colored"

Dotenv.load

LOGGER = Logger.new(ENV.key?("LOG_TO_STDERR") ? $stderr : "test-#{Time.now.to_i}.log")
LOGGER.level = Logger.const_get(ENV.fetch("LOG_LEVEL", "INFO"))

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

    "topic-#{@@topic_number}"
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
      let(:kafka_brokers) { ["localhost:9092", "localhost:9093", "localhost:9094"] }
      let(:kafka) { Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger) }

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
