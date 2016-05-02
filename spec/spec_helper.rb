$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "active_support/notifications"
require "kafka"
require "dotenv"
require "logger"
require "rspec-benchmark"
require "colored"

Dotenv.load

require "test_cluster"

LOGGER = Logger.new(ENV.key?("LOG_TO_STDERR") ? $stderr : nil)
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

  def create_topic(*args)
    cluster.create_topic(*args)
  end
end

KAFKA_CLUSTER = TestCluster.new

module FunctionalSpecHelpers
  def self.included(base)
    base.class_eval do
      let(:logger) { LOGGER }
      let(:kafka) { Kafka.new(seed_brokers: kafka_brokers, client_id: "test", logger: logger) }
      let(:cluster) { KAFKA_CLUSTER }
      let(:kafka_brokers) { cluster.kafka_hosts }

      after { kafka.close }
    end
  end
end

RSpec.configure do |config|
  config.filter_run_excluding functional: true, performance: true, fuzz: true
  config.include RSpec::Benchmark::Matchers
  config.include SpecHelpers
  config.include FunctionalSpecHelpers, functional: true
  config.include FunctionalSpecHelpers, fuzz: true

  config.before(:suite) do
    if config.inclusion_filter[:functional] || config.inclusion_filter[:fuzz]
      KAFKA_CLUSTER.start
    end
  end

  config.after(:suite) do
    if config.inclusion_filter[:functional] || config.inclusion_filter[:fuzz]
      KAFKA_CLUSTER.stop
    end
  end
end

ActiveSupport::Notifications.subscribe(/.*\.kafka$/) do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  LOGGER.debug "Instrumentation event `#{event.name}`: #{event.payload.inspect}"
end
