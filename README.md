# Kafka

A Ruby client library for the Kafka distributed log system. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.

This library is still in pre-beta stage, but development is ongoing. Current efforts are focused on implementing a solid Producer client. The next step will be implementing a client for the Kafka 0.9 Consumer API.

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

Currently, only the Producer API is supported. A Kafka 0.9 compatible Consumer API is on the roadmap.

```ruby
# A client must be initialized with at least one Kafka broker. Each client keeps
# a separate pool of broker connections. Don't use the same client from more than
# one thread.
kafka = Kafka.new(
  seed_brokers: ["kafka1:9092", "kafka2:9092"],
  client_id: "my-app",
  logger: Logger.new($stderr),
)

# A producer buffers messages and sends them to the broker that is the leader of
# the partition a given message is being produced to.
producer = kafka.get_producer

# `produce` will buffer the message in the producer.
producer.produce("hello1", key: "x", topic: "test-messages", partition: 0)
producer.produce("hello2", key: "y", topic: "test-messages", partition: 1)

# `send_messages` will send the buffered messages to the cluster. Since messages
# may be destined for different partitions, this could involve writing to more
# than one Kafka broker.
producer.send_messages
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

**Note:** the specs require a working [Docker](https://www.docker.com/) instance, but should work out of the box if you have Docker installed. Please create an issue if that's not the case.

## Roadmap

v0.1 is targeted for release in February. Other milestones do not have firm target dates, but v0.2 will be released as soon as we are confident that it is ready to run in critical production environments and that the API shouldn't be changed.

### v0.1: Producer API for non-critical production data

We need to actually run this in production for a while before we can say that it won't lose data, so initially the library should only be deployed for non-critical use cases.

The API may also be changed.

### v0.2: Stable Producer API

The API should now have stabilized and the library should be battle tested enough to deploy for critical use cases.

### v1.0: Consumer API

The Consumer API defined by Kafka 0.9 will be implemented.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/ruby-kafka.


## Copyright and license

Copyright 2015 Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
