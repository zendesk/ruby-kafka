# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "logger"
require "active_support"
# A missing require for try has been added to rails master here:
#   https://github.com/rails/rails/commit/530f7805ed5790af1d472a041bc74089dc183f47
# The explicit require below can be removed with a future Rails release (6.1?).
require "active_support/core_ext/object/try"
require "active_support/notifications"
require "kafka"
require "kafka/tagged_logger"
require "dotenv"
require "logger"
require "rspec-benchmark"
require "colored"
require "securerandom"
require 'snappy'
require 'extlz4'
require 'zstd-ruby'

Dotenv.load

LOGGER = Kafka::TaggedLogger.new(Logger.new(ENV.key?("LOG_TO_STDERR") ? $stderr : "test-#{Time.now.to_i}.log"))
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
    "#{RUN_ID}-topic-#{SecureRandom.uuid}"
  end

  def create_random_topic(**args)
    topic = generate_topic_name
    create_topic(topic, **args)
    topic
  end

  def create_topic(name, num_partitions: 1, num_replicas: 1)
    kafka.create_topic(name, num_partitions: num_partitions, replication_factor: num_replicas)

    # Every broker must know the topic (including for regex subscriptions),
    # and every partition leader must serve usable offsets before consumers start.
    expected_offsets = (0...num_partitions).map { |partition| [partition, -1] }.to_h
    kafka.brokers.each do |broker|
      client = Kafka.new(["#{broker.host}:#{broker.port}"], logger: LOGGER)
      begin
        with_retry(errors: [Kafka::NotLeaderForPartition]) do
          expect(client.has_topic?(name)).to eq(true) # Refresh metadata on every attempt.
          expect(client.last_offsets_for(name).fetch(name)).to eq(expected_offsets)
        end
      ensure
        client.close
      end
    end
  end
end

module FunctionalSpecHelpers
  # Keep rebalances responsive without changing the library's production defaults.
  # Multi-consumer tests must not process records before all members have joined.
  def create_consumer(client = kafka, group_members: 1, **options)
    if group_members > 1
      strategy = options.fetch(:assignment_strategy) { Kafka::RoundRobinAssignmentStrategy.new }
      group_id = options.fetch(:group_id)
      allow(strategy).to receive(:call).and_wrap_original do |method, **args|
        ready = started_consumer_groups[group_id] ||= args.fetch(:members).size >= group_members
        ready ? method.call(**args) : {}
      end
      options[:assignment_strategy] = strategy
    end

    client.consumer(heartbeat_interval: 0.1, **options)
  end

  def fetch_records(topic:, partition: 0, offset: :earliest, count:)
    with_retry do
      records = kafka.fetch_messages(topic: topic, partition: partition, offset: offset, max_wait_time: 0.1)
      expect(records.size).to eq(count)
      records
    end
  end

  # KRaft metadata changes and transaction completion are asynchronous.
  def with_retry(opts = {})
    retries = opts.fetch(:retry, 30)
    retry_wait = opts.fetch(:retry_wait, 0.1)
    errors = opts.fetch(:errors, [])
    tries = 0
    begin
      yield
    rescue RSpec::Expectations::ExpectationNotMetError, Kafka::UnknownTopicOrPartition, Kafka::LeaderNotAvailable, *errors
      raise if tries >= retries

      tries += 1
      sleep retry_wait
      retry
    end
  end

  def self.included(base)
    base.class_eval do
      let(:logger) { LOGGER }
      let(:kafka_brokers) { KAFKA_BROKERS }
      let(:kafka) { Kafka.new(kafka_brokers, client_id: "test", logger: logger) }
      let(:started_consumer_groups) { {} }

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
