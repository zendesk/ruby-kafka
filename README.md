# ruby-kafka

[![Circle CI](https://circleci.com/gh/zendesk/ruby-kafka.svg?style=shield)](https://circleci.com/gh/zendesk/ruby-kafka/tree/master)

A Ruby client library for the Kafka distributed log system. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.

Currently, only the Producer API has been implemented, but a fully-fledged Consumer implementation compatible with Kafka 0.9 is on the roadmap.

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
require "kafka"

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
producer.produce("hello1", topic: "test-messages")

# It's possible to specify a message key:
producer.produce("hello2", key: "x", topic: "test-messages")

# If you need to control which partition a message should be written to, you
# can pass in the `partition` parameter:
producer.produce("hello3", topic: "test-messages", partition: 1)

# If you don't know exactly how many partitions are in the topic, or you'd
# rather have some level of indirection, you can pass in `partition_key`.
# Two messages with the same partition key will always be written to the
# same partition.
producer.produce("hello4", topic: "test-messages", partition_key: "yo")

# `send_messages` will send the buffered messages to the cluster. Since messages
# may be destined for different partitions, this could involve writing to more
# than one Kafka broker.
producer.send_messages
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

**Note:** the specs require a working [Docker](https://www.docker.com/) instance, but should work out of the box if you have Docker installed. Please create an issue if that's not the case.

## Roadmap

The current stable release is v0.1. This release is running in production at Zendesk, but it's still not recommended that you use it when data loss is unacceptable. It will take a little while until all edge cases have been uncovered and handled.

The API may still be changed in v0.2. 

### v0.2: Stable Producer API

Target date: end of February.

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
