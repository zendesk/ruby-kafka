# ruby-kafka

[![Circle CI](https://circleci.com/gh/zendesk/ruby-kafka.svg?style=shield)](https://circleci.com/gh/zendesk/ruby-kafka/tree/master)

A Ruby client library for [Apache Kafka](http://kafka.apache.org/), a distributed log and message bus. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.

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

Please see the [documentation site](http://www.rubydoc.info/gems/ruby-kafka) for detailed documentation on the latest release.

### Producing Messages to Kafka

A client must be initialized with at least one Kafka broker. Each client keeps a separate pool of broker connections. Don't use the same client from more than one thread.

```ruby
require "kafka"

kafka = Kafka.new(seed_brokers: ["kafka1:9092", "kafka2:9092"])
```

A producer buffers messages and sends them to the broker that is the leader of the partition a given message is assigned to.

```ruby
producer = kafka.get_producer
```

`produce` will buffer the message in the producer but will _not_ actually send it to the Kafka cluster.

```ruby
producer.produce("hello1", topic: "test-messages")
```

It's possible to specify a message key.

```ruby
producer.produce("hello2", key: "x", topic: "test-messages")
```

If you need to control which partition a message should be assigned to, you can pass in the `partition` parameter.

```ruby
producer.produce("hello3", topic: "test-messages", partition: 1)
```

If you don't know exactly how many partitions are in the topic, or you'd rather have some level of indirection, you can pass in `partition_key`. Two messages with the same partition key will always be assigned to the same partition.

```ruby
producer.produce("hello4", topic: "test-messages", partition_key: "yo")
```

`send_messages` will send the buffered messages to the cluster. Since messages may be destined for different partitions, this could involve writing to more than one Kafka broker. Note that a failure to send all buffered messages after the configured number of retries will result in `Kafka::FailedToSendMessages` being raised. This can be rescued and ignored; the messages will be kept in the buffer until the next attempt.

```ruby
producer.send_messages
```

Read the docs for [Kafka::Producer](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Producer) for more details.

### Buffering and Error Handling

The producer is designed for resilience in the face of temporary network errors, Kafka broker failovers, and other issues that prevent the client from writing messages to the destination topics. It does this by employing local, in-memory buffers. Only when messages are acknowledged by a Kafka broker will they be removed from the buffer.

Typically, you'd configure the producer to retry failed attempts at sending messages, but sometimes all retries are exhausted. In that case, `Kafka::FailedToSendMessages` is raised from `Kafka::Producer#send_messages`. If you wish to have your application be resilient to this happening (e.g. if you're logging to Kafka from a web application) you can rescue this exception. The failed messages are still retained in the buffer, so a subsequent call to `#send_messages` will still attempt to send them.

Note that there's a maximum buffer size; pass in a different value for `max_buffer_size` when calling `#get_producer` in order to configure this.

A final note on buffers: local buffers give resilience against broker and network failures, and allow higher throughput due to message batching, but they also trade off consistency guarantees for higher availibility and resilience. If your local process dies while messages are buffered, those messages will be lost. If you require high levels of consistency, you should call `#send_messages` immediately after `#produce`.

### Understanding Timeouts

It's important to understand how timeouts work if you have a latency sensitive application. This library allows configuring timeouts on different levels:

**Network timeouts** apply to network connections to individual Kafka brokers. There are two config keys here, each passed to `Kafka.new`:

* `connect_timeout` sets the number of seconds to wait while connecting to a broker for the first time. When ruby-kafka initializes, it needs to connect to at least one host in `seed_brokers` in order to discover the Kafka cluster. Each host is tried until there's one that works. Usually that means the first one, but if your entire cluster is down, or there's a network partition, you could wait up to `n * connect_timeout` seconds, where `n` is the number of seed brokers.
* `socket_timeout` sets the number of seconds to wait when reading from or writing to a socket connection to a broker. After this timeout expires the connection will be killed. Note that some Kafka operations are by definition long-running, such as waiting for new messages to arrive in a partition, so don't set this value too low. When configuring timeouts relating to specific Kafka operations, make sure to make them shorter than this one.

**Producer timeouts** can be configured when calling `#get_producer` on a client instance:

* `ack_timeout` is a timeout executed by a broker when the client is sending messages to it. It defines the number of seconds the broker should wait for replicas to acknowledge the write before responding to the client with an error. As such, it relates to the `required_acks` setting. It should be set lower than `socket_timeout`.
* `retry_backoff` configures the number of seconds to wait after a failed attempt to send messages to a Kafka broker before retrying. The `max_retries` setting defines the maximum number of retries to attempt, and so the total duration could be up to `max_retries * retry_backoff` seconds. The timeout can be arbitrarily long, and shouldn't be too short: if a broker goes down its partitions will be handed off to another broker, and that can take tens of seconds.

When sending many messages, it's likely that the client needs to send some messages to each broker in the cluster. Given `n` brokers in the cluster, the total wait time when calling `Kafka::Producer#send_messages` can be up to

    n * (connect_timeout + socket_timeout + retry_backoff) * max_retries

Make sure your application can survive being blocked for so long.

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

## Why a new library?

There are a few existing Kafka clients in Ruby:

* [Poseidon](https://github.com/bpot/poseidon) seems to work for Kafka 0.8, but the project has is unmaintained and has known issues.
* [Hermann](https://github.com/reiseburo/hermann) wraps the C library [librdkafka](https://github.com/edenhill/librdkafka) and seems to be very efficient, but its API and mode of operation is too intrusive for our needs.
* [jruby-kafka](https://github.com/joekiller/jruby-kafka) is a great option if you're running on JRuby.

We needed a robust client that could be used from our existing Ruby apps, allowed our Ops to monitor operation, and provided flexible error handling. There didn't exist such a client, hence this project.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/zendesk/ruby-kafka.


## Copyright and license

Copyright 2015 Zendesk

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
