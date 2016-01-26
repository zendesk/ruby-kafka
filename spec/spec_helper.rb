$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "kafka"
require "dotenv"
require "logger"

Dotenv.load

LOG = ENV.key?("LOG_TO_STDERR") ? $stderr : StringIO.new

RSpec.configure do |config|
  config.filter_run_excluding type: :functional
end
