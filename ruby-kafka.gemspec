# coding: utf-8
# frozen_string_literal: true

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka/version'

Gem::Specification.new do |spec|
  spec.name          = "ruby-kafka"
  spec.version       = Kafka::VERSION
  spec.authors       = ["Daniel Schierbeck"]
  spec.email         = ["daniel.schierbeck@gmail.com"]

  spec.summary       = "A client library for the Kafka distributed commit log."

  spec.description   = <<-DESC.gsub(/^    /, "").strip
    A client library for the Kafka distributed commit log.
  DESC

  spec.homepage      = "https://github.com/zendesk/ruby-kafka"
  spec.license       = "Apache-2.0"

  spec.required_ruby_version = '>= 2.1.0'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'digest-crc'

  spec.add_development_dependency "bundler", ">= 1.9.5"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "digest-murmurhash"
  spec.add_development_dependency "dotenv"
  spec.add_development_dependency "docker-api"
  spec.add_development_dependency "rspec-benchmark"
  spec.add_development_dependency "activesupport", ">= 4.0", "< 6.1"
  spec.add_development_dependency "snappy"
  spec.add_development_dependency "extlz4"
  spec.add_development_dependency "zstd-ruby"
  spec.add_development_dependency "colored"
  spec.add_development_dependency "rspec_junit_formatter", "0.2.2"
  spec.add_development_dependency "dogstatsd-ruby", ">= 4.0.0", "< 5.0.0"
  spec.add_development_dependency "statsd-ruby"
  spec.add_development_dependency "prometheus-client", "~> 0.10.0"
  spec.add_development_dependency "ruby-prof"
  spec.add_development_dependency "timecop"
  spec.add_development_dependency "rubocop", "~> 0.49.1"
  spec.add_development_dependency "gssapi", ">= 1.2.0"
  spec.add_development_dependency "stackprof"
end
