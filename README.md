# Kafka

A Ruby client library for the Kafka distributed log system. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.

This library is still in pre-alpha stage, but development is ongoing. Current efforts are focused on implementing a solid Producer client. The next step will be implementing a client for the Kafka 0.9 Consumer API.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ruby-kafka'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ruby-kafka

## Usage

```ruby
kafka = Kafka.new(
  seed_brokers: ["kafka1:9092", "kafka2:9092"],
  client_id: "my-app",
  logger: Logger.new($stderr),
)

producer = kafka.get_producer

# `write` will buffer the message in the producer.
producer.write("hello1", key: "x", topic: "test-messages", partition: 0)
producer.write("hello2", key: "y", topic: "test-messages", partition: 1)

# `flush` will send the buffered messages to the cluster.
producer.flush
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/ruby-kafka.


## Copyright and license

Copyright 2015 Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
