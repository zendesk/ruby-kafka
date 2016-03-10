# ruby-kafka

A Ruby client library for [Apache Kafka](http://kafka.apache.org/), a distributed log and message bus. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.

The Producer API is currently beta level and used in production. There's an alpha level Consumer Group API that has not yet been used in production and that may change without warning. Feel free to try it out but don't expect it to be stable or correct quite yet.

Although parts of this library work with Kafka 0.8 – specifically, the Producer API – it's being tested and developed against Kafka 0.9. The Consumer API will be 0.9 only.

#### Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)
  1. [Producing Messages to Kafka](#producing-messages-to-kafka)
    1. [Asynchronously Producing Messages](#asynchronously-producing-messages)
    2. [Serialization](#serialization)
    3. [Partitioning](#partitioning)
    4. [Buffering and Error Handling](#buffering-and-error-handling)
    5. [Message Delivery Guarantees](#message-delivery-guarantees)
    6. [Compression](#compression)
  2. [Consuming Messages from Kafka](#consuming-messages-from-kafka)
  3. [Logging](#logging)
  4. [Instrumentation](#instrumentation)
  5. [Understanding Timeouts](#understanding-timeouts)
  6. [Encryption and Authentication using SSL](#encryption-and-authentication-using-ssl)
3. [Development](#development)
4. [Roadmap](#roadmap)

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

Please see the [documentation site](http://www.rubydoc.info/gems/ruby-kafka) for detailed documentation on the latest release. Note that the documentation on GitHub may not match the version of the library you're using – there are still being made many changes to the API.

### Producing Messages to Kafka

A client must be initialized with at least one Kafka broker. Each client keeps a separate pool of broker connections. Don't use the same client from more than one thread.

```ruby
require "kafka"

kafka = Kafka.new(seed_brokers: ["kafka1:9092", "kafka2:9092"])
```

A producer buffers messages and sends them to the broker that is the leader of the partition a given message is assigned to.

```ruby
producer = kafka.producer
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

`deliver_messages` will send the buffered messages to the cluster. Since messages may be destined for different partitions, this could involve writing to more than one Kafka broker. Note that a failure to send all buffered messages after the configured number of retries will result in `Kafka::DeliveryFailed` being raised. This can be rescued and ignored; the messages will be kept in the buffer until the next attempt.

```ruby
producer.deliver_messages
```

Read the docs for [Kafka::Producer](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Producer) for more details.

#### Asynchronously Producing Messages

A normal producer will block while `#deliver_messages` is sending messages to Kafka, possible for tens of seconds or even minutes at a time, depending on your timeout and retry settings. Furthermore, you have to call `#deliver_messages` manually, with a frequency that balances batch size with message delay.

In order to avoid blocking during message deliveries you can use the _asynchronous producer_ API. It is mostly similar to the synchronous API, with calls to `#produce` and `#deliver_messages`. The main difference is that rather than blocking, these calls will return immediately. The actual work will be done in a background thread, with the messages and operations being sent from the caller over a thread safe queue.

```ruby
# `#async_producer` will create a new asynchronous producer.
producer = kafka.async_producer

# The `#produce` API works as normal.
producer.produce("hello", topic: "greetings")

# `#deliver_messages` will return immediately.
producer.deliver_messages

# Make sure to call `#shutdown` on the producer in order to
# avoid leaking resources.
producer.shutdown
```

By default, the delivery policy will be the same as for a synchronous producer: only when `#deliver_messages` is called will the messages be delivered. However, the asynchronous producer offers two complementary policies for _automatic delivery_:

1. Trigger a delivery once the producer's message buffer reaches a specified _threshold_. This can be used to improve efficiency by increasing the batch size when sending messages to the Kafka cluster.
2. Trigger a delivery at a _fixed time interval_. This puts an upper bound on message delays.

These policies can be used alone or in combination.

```ruby
# `async_producer` will create a new asynchronous producer.
producer = kafka.async_producer(
  # Trigger a delivery once 100 messages have been buffered.
  delivery_threshold: 100,
  
  # Trigger a delivery every 30 seconds.
  delivery_interval: 30,
)

producer.produce("hello", topic: "greetings")

# ...
```

**Note:** if the calling thread produces messages faster than the producer can write them to Kafka, you'll eventually run into problems. The internal queue used for sending messages from the calling thread to the background worker has a size limit; once this limit is reached, a call to `#produce` will raise `Kafka::BufferOverflow`.

#### Serialization

This library is agnostic to which serialization format you prefer. Both the value and key of a message is treated as a binary string of data. This makes it easier to use whatever serialization format you want, since you don't have to do anything special to make it work with ruby-kafka. Here's an example of encoding data with JSON:

```ruby
require "json"

# ...

event = {
  "name" => "pageview",
  "url" => "https://example.com/posts/123",
  # ...
}

data = JSON.dump(event)

producer.produce(data, topic: "events")
```

#### Partitioning

Kafka topics are partitioned, with messages being assigned to a partition by the client. This allows a great deal of flexibility for the users. This section describes several strategies for partitioning and how they impact performance, data locality, etc.


##### Load Balanced Partitioning

When optimizing for efficiency, we either distribute messages as evenly as possible to all partitions, or make sure each producer always writes to a single partition. The former ensures an even load for downstream consumers; the latter ensures the highest producer performance, since message batching is done per partition.

If no explicit partition is specified, the producer will look to the partition key or the message key for a value that can be used to deterministically assign the message to a partition. If there is a big number of different keys, the resulting distribution will be pretty even. If no keys are passed, the producer will randomly assign a partition. Random partitioning can be achieved even if you use message keys by passing a random partition key, e.g. `partition_key: rand(100)`.

If you wish to have the producer write all messages to a single partition, simply generate a random value and re-use that as the partition key:

```ruby
partition_key = rand(100)

producer.produce(msg1, topic: "messages", partition_key: partition_key)
producer.produce(msg2, topic: "messages", partition_key: partition_key)

# ...
```

You can also base the partition key on some property of the producer, for example the host name.

##### Semantic Partitioning

By assigning messages to a partition based on some property of the message, e.g. making sure all events tracked in a user session are assigned to the same partition, downstream consumers can make simplifying assumptions about data locality. In this example, a consumer can keep process local state pertaining to a user session knowing that all events for the session will be read from a single partition. This is also called _semantic partitioning_, since the partition assignment is part of the application behavior.

Typically it's sufficient to simply pass a partition key in order to guarantee that a set of messages will be assigned to the same partition, e.g.

```ruby
# All messages with the same `session_id` will be assigned to the same partition.
producer.produce(event, topic: "user-events", partition_key: session_id)
```

However, sometimes it's necessary to select a specific partition. When doing this, make sure that you don't pick a partition number outside the range of partitions for the topic:

```ruby
partitions = kafka.partitions_for("events")

# Make sure that we don't exceed the partition count!
partition = some_number % partitions

producer.produce(event, topic: "events", partition: partition)
```

##### Compatibility with Other Clients

There's no standardized way to assign messages to partitions across different Kafka client implementations. If you have a heterogeneous set of clients producing messages to the same topics it may be important to ensure a consistent partitioning scheme. This library doesn't try to implement all schemes, so you'll have to figure out which scheme the other client is using and replicate it. An example:

```ruby
partitions = kafka.partitions_for("events")

# Insert your custom partitioning scheme here:
partition = PartitioningScheme.assign(partitions, event)

producer.produce(event, topic: "events", partition: partition)
```

#### Buffering and Error Handling

The producer is designed for resilience in the face of temporary network errors, Kafka broker failovers, and other issues that prevent the client from writing messages to the destination topics. It does this by employing local, in-memory buffers. Only when messages are acknowledged by a Kafka broker will they be removed from the buffer.

Typically, you'd configure the producer to retry failed attempts at sending messages, but sometimes all retries are exhausted. In that case, `Kafka::DeliveryFailed` is raised from `Kafka::Producer#deliver_messages`. If you wish to have your application be resilient to this happening (e.g. if you're logging to Kafka from a web application) you can rescue this exception. The failed messages are still retained in the buffer, so a subsequent call to `#deliver_messages` will still attempt to send them.

Note that there's a maximum buffer size; pass in a different value for `max_buffer_size` when calling `#producer` in order to configure this.

A final note on buffers: local buffers give resilience against broker and network failures, and allow higher throughput due to message batching, but they also trade off consistency guarantees for higher availibility and resilience. If your local process dies while messages are buffered, those messages will be lost. If you require high levels of consistency, you should call `#deliver_messages` immediately after `#produce`.

#### Message Delivery Guarantees

There are basically two different and incompatible guarantees that can be made in a message delivery system such as Kafka:

1. _at-most-once_ delivery guarantees that a message is at most delivered to the recipient _once_. This is useful only if delivering the message twice carries some risk and should be avoided. Implicit is the fact that there's no guarantee that the message will be delivered at all.
2. _at-least-once_ delivery guarantees that a message is delivered, but it may be delivered more than once. If the final recipient de-duplicates messages, e.g. by checking a unique message id, then it's even possible to implement _exactly-once_ delivery.

Of these two options, ruby-kafka implements the second one: when in doubt about whether a message has been delivered, a producer will try to deliver it again.

The guarantee is made only for the synchronous producer and boils down to this:

```ruby
producer = kafka.producer

producer.produce("hello", topic: "greetings")

# If this line fails with Kafka::DeliveryFailed we *may* have succeeded in deliverying
# the message to Kafka but won't know for sure.
producer.deliver_messages

# If we get to this line we can be sure that the message has been delivered to Kafka!
```

That is, once `#deliver_messages` returns we can be sure that Kafka has received the message. Note that there are some big caveats here:

- Depending on how your cluster and topic is configured the message could still be lost by Kafka.
- If you configure the producer to not require acknowledgements from the Kafka brokers by setting `required_acks` to zero there is no guarantee that the messsage will ever make it to a Kafka broker.
- If you use the asynchronous producer there's no guarantee that messages will have been delivered after `#deliver_messages` returns. A way of blocking until a message has been delivered with the asynchronous producer may be implemented in the future.

#### Compression

Depending on what kind of data you produce, enabling compression may yield improved bandwidth and space usage. Compression in Kafka is done on entire messages sets rather than on individual messages. This improves the compression rate and generally means that compressions works better the larger your buffers get, since the message sets will be larger by the time they're compressed.

Since many workloads have variations in throughput and distribution across partitions, it's possible to configure a threshold for when to enable compression by setting `compression_threshold`. Only if the defined number of messages are buffered for a partition will the messages be compressed.

Compression is enabled by passing the `compression_codec` parameter to `#producer` with the name of one of the algorithms allowed by Kafka:

* `:snappy` for [Snappy](http://google.github.io/snappy/) compression.
* `:gzip` for [gzip](https://en.wikipedia.org/wiki/Gzip) compression.

By default, all message sets will be compressed if you specify a compression codec. To increase the compression threshold, set `compression_threshold` to an integer value higher than one.

```ruby
producer = kafka.producer(
  compression_codec: :snappy,
  compression_threshold: 10,
)
```

### Consuming Messages from Kafka

**Warning:** The Consumer API is still alpha level and will likely change. The consumer code should not be considered stable, as it hasn't been exhaustively tested in production environments yet.

The simplest way to consume messages from a Kafka topic is using the `#fetch_messages` API:

```ruby
require "kafka"

kafka = Kafka.new(seed_brokers: ["kafka1:9092", "kafka2:9092"])

messages = kafka.fetch_messages(topic: "greetings", partition: 42)

messages.each do |message|
  puts message.offset, message.key, message.value
end
```

While this is great for extremely simple use cases, there are a number of downsides:

- You can only fetch from a single topic and partition at a time.
- If you want to have multiple processes consume from the same topic, there's no way of coordinating which processes should fetch from which partitions.
- If a process dies, there's no way to have another process resume fetching from the point in the partition that the original process had reached.

The Consumer API solves all of these issues, and more. It uses the Consumer Groups feature released in Kafka 0.9 to allow multiple consumer processes to coordinate access to a topic, assigning each partition to a single consumer. When a consumer fails, the partitions that were assigned to it are re-assigned to other members of the group.

Using the API is simple:

```ruby
require "kafka"

kafka = Kafka.new(seed_brokers: ["kafka1:9092", "kafka2:9092"])

# Consumers with the same group id will form a Consumer Group together.
consumer = kafka.consumer(group_id: "my-consumer")

# It's possible to subscribe to multiple topics by calling `subscribe`
# repeatedly.
consumer.subscribe("greetings")

# This will loop indefinitely, yielding each message in turn.
consumer.each_message do |message|
  puts message.topic, message.partition
  puts message.offset, message.key, message.value
end
```

Each consumer process will be assigned one or more partitions from each topic that the group subscribes to. In order to handle more messages, simply start more processes.


### Logging

It's a very good idea to configure the Kafka client with a logger. All important operations and errors are logged. When instantiating your client, simply pass in a valid logger:

```ruby
logger = Logger.new("log/kafka.log")
kafka = Kafka.new(logger: logger, ...)
```

By default, nothing is logged.

### Instrumentation

Most operations are instrumented using [Active Support Notifications](http://api.rubyonrails.org/classes/ActiveSupport/Notifications.html). In order to subscribe to notifications, make sure to require the notifications library _before_ you require ruby-kafka, e.g.

```ruby
require "active_support/notifications"
require "kafka"
```

The notifications are namespaced based on their origin, with separate namespaces for the producer and the consumer.

In order to receive notifications you can either subscribe to individual notification names or use regular expressions to subscribe to entire namespaces. This example will subscribe to _all_ notifications sent by ruby-kafka:

```ruby
ActiveSupport::Notifications.subscribe(/.*\.kafka/) do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  puts "Received notification `#{event.name}` with payload: #{event.payload.inspect}"
end
```

#### Producer Notifications

* `produce_message.producer.kafka` is sent whenever a message is produced to a buffer. It includes the following payload:
  * `value` is the message value.
  * `key` is the message key.
  * `topic` is the topic that the message was produced to.
  * `buffer_size` is the size of the producer buffer after adding the message.
  * `max_buffer_size` is the maximum size of the producer buffer.

* `deliver_messages.producer.kafka` is sent whenever a producer attempts to deliver its buffered messages to the Kafka brokers. It includes the following payload:
  * `attempts` is the number of times delivery was attempted.
  * `message_count` is the number of messages for which delivery was attempted.
  * `delivered_message_count` is the number of messages that were acknowledged by the brokers - if this number is smaller than `message_count` not all messages were successfully delivered.

#### Connection Notifications

* `request.connection.kafka` is sent whenever a network request is sent to a Kafka broker. It includes the following payload:
  * `api` is the name of the API that was called, e.g. `produce` or `fetch`.
  * `request_size` is the number of bytes in the request.
  * `response_size` is the number of bytes in the response.

### Understanding Timeouts

It's important to understand how timeouts work if you have a latency sensitive application. This library allows configuring timeouts on different levels:

**Network timeouts** apply to network connections to individual Kafka brokers. There are two config keys here, each passed to `Kafka.new`:

* `connect_timeout` sets the number of seconds to wait while connecting to a broker for the first time. When ruby-kafka initializes, it needs to connect to at least one host in `seed_brokers` in order to discover the Kafka cluster. Each host is tried until there's one that works. Usually that means the first one, but if your entire cluster is down, or there's a network partition, you could wait up to `n * connect_timeout` seconds, where `n` is the number of seed brokers.
* `socket_timeout` sets the number of seconds to wait when reading from or writing to a socket connection to a broker. After this timeout expires the connection will be killed. Note that some Kafka operations are by definition long-running, such as waiting for new messages to arrive in a partition, so don't set this value too low. When configuring timeouts relating to specific Kafka operations, make sure to make them shorter than this one.

**Producer timeouts** can be configured when calling `#producer` on a client instance:

* `ack_timeout` is a timeout executed by a broker when the client is sending messages to it. It defines the number of seconds the broker should wait for replicas to acknowledge the write before responding to the client with an error. As such, it relates to the `required_acks` setting. It should be set lower than `socket_timeout`.
* `retry_backoff` configures the number of seconds to wait after a failed attempt to send messages to a Kafka broker before retrying. The `max_retries` setting defines the maximum number of retries to attempt, and so the total duration could be up to `max_retries * retry_backoff` seconds. The timeout can be arbitrarily long, and shouldn't be too short: if a broker goes down its partitions will be handed off to another broker, and that can take tens of seconds.

When sending many messages, it's likely that the client needs to send some messages to each broker in the cluster. Given `n` brokers in the cluster, the total wait time when calling `Kafka::Producer#deliver_messages` can be up to

    n * (connect_timeout + socket_timeout + retry_backoff) * max_retries

Make sure your application can survive being blocked for so long.

### Encryption and Authentication using SSL

By default, communication between Kafka clients and brokers is unencrypted and unauthenticated. Kafka 0.9 added optional support for [encryption and client authentication and authorization](http://kafka.apache.org/documentation.html#security_ssl). There are two layers of security made possible by this:

#### Encryption of Communication

By enabling SSL encryption you can have some confidence that messages can be sent to Kafka over an untrusted network without being intercepted.

In this case you just need to pass a valid CA certificate as a string when configuring your `Kafka` client:

```ruby
kafka = Kafka.new(
  ssl_ca_cert: File.read('my_ca_cert.pem'),
  # ...
)
```

Without passing the CA certificate to the client it would be impossible to protect against [man-in-the-middle attacks](https://en.wikipedia.org/wiki/Man-in-the-middle_attack).

#### Client Authentication

In order to authenticate the client to the cluster, you need to pass in a certificate and key created for the client and trusted by the brokers.

```ruby
kafka = Kafka.new(
  ssl_ca_cert: File.read('my_ca_cert.pem'),
  ssl_client_cert: File.read('my_client_cert.pem'),
  ssl_client_cert_key: File.read('my_client_cert_key.pem'),
  # ...
)
```

Once client authentication is set up, it is possible to configure the Kafka cluster to [authorize client requests](http://kafka.apache.org/documentation.html#security_authz).

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

**Note:** the specs require a working [Docker](https://www.docker.com/) instance, but should work out of the box if you have Docker installed. Please create an issue if that's not the case.

[![Circle CI](https://circleci.com/gh/zendesk/ruby-kafka.svg?style=shield)](https://circleci.com/gh/zendesk/ruby-kafka/tree/master)

## Roadmap

The current stable release is v0.2. This release is running in production at Zendesk, but it's still not recommended that you use it when data loss is unacceptable. It will take a little while until all edge cases have been uncovered and handled.

### v0.3

Beta release of the Consumer API, allowing balanced Consumer Groups coordinating access to partitions. Kafka 0.9 only.

### v1.0

API freeze. All new changes will be backwards compatible.

## Why a new library?

There are a few existing Kafka clients in Ruby:

* [Poseidon](https://github.com/bpot/poseidon) seems to work for Kafka 0.8, but the project is unmaintained and has known issues.
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
