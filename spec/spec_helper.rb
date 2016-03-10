$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "active_support/notifications"
require "kafka"
require "dotenv"
require "logger"
require "rspec-benchmark"

Dotenv.load

LOG = ENV.key?("LOG_TO_STDERR") ? $stderr : StringIO.new

RSpec.configure do |config|
  config.filter_run_excluding functional: true, performance: true, fuzz: true
  config.include RSpec::Benchmark::Matchers
end

logger = Logger.new(LOG)

ActiveSupport::Notifications.subscribe(/.*\.kafka$/) do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  logger.debug "Instrumentation event `#{event.name}`: #{event.payload.inspect}"
end
