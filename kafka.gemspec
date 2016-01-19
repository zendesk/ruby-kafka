# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka/version'

Gem::Specification.new do |spec|
  spec.name          = "ruby-kafka"
  spec.version       = Kafka::VERSION
  spec.authors       = ["Daniel Schierbeck"]
  spec.email         = ["daniel.schierbeck@gmail.com"]

  spec.summary       = %q{A client library for the Kafka distributed commit log.}
  spec.description   = %q{A client library for the Kafka distributed commit log. Still very much at the alpha stage.}
  spec.homepage      = "https://github.com/zendesk/ruby-kafka"
  spec.license       = "Apache License Version 2.0"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.10"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec"
end
