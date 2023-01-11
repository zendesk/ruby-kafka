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
  spec.license       = "Apache License Version 2.0"

  spec.required_ruby_version = '>= 2.1.0'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end
