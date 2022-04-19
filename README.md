# ruby-kafka

A Ruby client library for [Apache Kafka](http://kafka.apache.org/), a distributed log and message bus. The focus of this library will be operational simplicity, with good logging and metrics that can make debugging issues easier.


## Table of Contents

1. [Installation](#installation)
2. [Compatibility](#compatibility)
3. [Usage](#usage)
    1. [Setting up the Kafka Client](#setting-up-the-kafka-client)
    2. [Producing Messages to Kafka](#producing-messages-to-kafka)
        1. [Efficiently Producing Messages](#efficiently-producing-messages)
        1. [Asynchronously Producing Messages](#asynchronously-producing-messages)
        2. [Serialization](#serialization)
        3. [Partitioning](#partitioning)
        4. [Buffering and Error Handling](#buffering-and-error-handling)
        5. [Message Durability](#message-durability)
        6. [Message Delivery Guarantees](#message-delivery-guarantees)
        7. [Compression](#compression)
        8. [Producing Messages from a Rails Application](#producing-messages-from-a-rails-application)
    3. [Consuming Messages from Kafka](#consuming-messages-from-kafka)
        1. [Consumer Groups](#consumer-groups)
        2. [Consumer Checkpointing](#consumer-checkpointing)
        3. [Topic Subscriptions](#topic-subscriptions)
        4. [Shutting Down a Consumer](#shutting-down-a-consumer)
        5. [Consuming Messages in Batches](#consuming-messages-in-batches)
        6. [Balancing Throughput and Latency](#balancing-throughput-and-latency)
        7. [Customizing Partition Assignment Strategy](#customizing-partition-assignment-strategy)
    4. [Thread Safety](#thread-safety)
    5. [Logging](#logging)
    6. [Instrumentation](#instrumentation)
    7. [Monitoring](#monitoring)
        1. [What to Monitor](#what-to-monitor)
        2. [Reporting Metrics to Statsd](#reporting-metrics-to-statsd)
        3. [Reporting Metrics to Datadog](#reporting-metrics-to-datadog)
    8. [Understanding Timeouts](#understanding-timeouts)
    9. [Security](#security)
        1. [Encryption and Authentication using SSL](#encryption-and-authentication-using-ssl)
        2. [Authentication using SASL](#authentication-using-sasl)
    10. [Topic management](#topic-management)
4. [Design](#design)
    1. [Producer Design](#producer-design)
    2. [Asynchronous Producer Design](#asynchronous-producer-design)
    3. [Consumer Design](#consumer-design)
5. [Development](#development)
6. [Support and Discussion](#support-and-discussion)
7. [Roadmap](#roadmap)
8. [Higher level libraries](#higher-level-libraries)
    1. [Message processing frameworks](#message-processing-frameworks)
    2. [Message publishing libraries](#message-publishing-libraries)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ruby-kafka'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install ruby-kafka

## Compatibility

<table>
  <tr>
    <th></th>
    <th>Producer API</th>
    <th>Consumer API</th>
  </tr>
  <tr>
    <th>Kafka 0.8</th>
    <td>Full support in v0.4.x</td>
    <td>Unsupported</td>
  </tr>
  <tr>
    <th>Kafka 0.9</th>
    <td>Full support in v0.4.x</td>
    <td>Full support in v0.4.x</td>
  </tr>
  <tr>
    <th>Kafka 0.10</th>
    <td>Full support in v0.5.x</td>
    <td>Full support in v0.5.x</td>
  </tr>
  <tr>
    <th>Kafka 0.11</th>
    <td>Full support in v0.7.x</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 1.0</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.0</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.1</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.2</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.3</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.4</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.5</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.6</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
  <tr>
    <th>Kafka 2.7</th>
    <td>Limited support</td>
    <td>Limited support</td>
  </tr>
</table>

This library is targeting Kafka 0.9 with the v0.4.x series and Kafka 0.10 with the v0.5.x series. There's limited support for Kafka 0.8, and things should work with Kafka 0.11, although there may be performance issues due to changes in the protocol.

- **Kafka 0.8:** Full support for the Producer API in ruby-kafka v0.4.x, but no support for consumer groups. Simple message fetching works.
- **Kafka 0.9:** Full support for the Producer and Consumer API in ruby-kafka v0.4.x.
- **Kafka 0.10:** Full support for the Producer and Consumer API in ruby-kafka v0.5.x. Note that you _must_ run version 0.10.1 or higher of Kafka due to limitations in 0.10.0.
- **Kafka 0.11:** Full support for Producer API, limited support for Consumer API in ruby-kafka v0.7.x. New features in 0.11.x includes new Record Batch format, idempotent and transactional production. The missing feature is dirty reading of Consumer API.
- **Kafka 1.0:** Everything that works with Kafka 0.11 should still work, but so far no features specific to Kafka 1.0 have been added.
- **Kafka 2.0:** Everything that works with Kafka 1.0 should still work, but so far no features specific to Kafka 2.0 have been added.
- **Kafka 2.1:** Everything that works with Kafka 2.0 should still work, but so far no features specific to Kafka 2.1 have been added.
- **Kafka 2.2:** Everything that works with Kafka 2.1 should still work, but so far no features specific to Kafka 2.2 have been added.
- **Kafka 2.3:** Everything that works with Kafka 2.2 should still work, but so far no features specific to Kafka 2.3 have been added.
- **Kafka 2.4:** Everything that works with Kafka 2.3 should still work, but so far no features specific to Kafka 2.4 have been added.
- **Kafka 2.5:** Everything that works with Kafka 2.4 should still work, but so far no features specific to Kafka 2.5 have been added.
- **Kafka 2.6:** Everything that works with Kafka 2.5 should still work, but so far no features specific to Kafka 2.6 have been added.
- **Kafka 2.7:** Everything that works with Kafka 2.6 should still work, but so far no features specific to Kafka 2.7 have been added.

This library requires Ruby 2.1 or higher.

## Usage

Please see the [documentation site](http://www.rubydoc.info/gems/ruby-kafka) for detailed documentation on the latest release. Note that the documentation on GitHub may not match the version of the library you're using – there are still being made many changes to the API.

### Setting up the Kafka Client

A client must be initialized with at least one Kafka broker, from which the entire Kafka cluster will be discovered. Each client keeps a separate pool of broker connections. Don't use the same client from more than one thread.

```ruby
require "kafka"

# The first argument is a list of "seed brokers" that will be queried for the full
# cluster topology. At least one of these *must* be available. `client_id` is
# used to identify this client in logs and metrics. It's optional but recommended.
kafka = Kafka.new(["kafka1:9092", "kafka2:9092"], client_id: "my-application")
```

You can also use a hostname with seed brokers' IP addresses:

```ruby
kafka = Kafka.new("seed-brokers:9092", client_id: "my-application", resolve_seed_brokers: true)
```

### Producing Messages to Kafka

The simplest way to write a message to a Kafka topic is to call `#deliver_message`:

```ruby
kafka = Kafka.new(...)
kafka.deliver_message("Hello, World!", topic: "greetings")
```

This will write the message to a random partition in the `greetings` topic. If you want to write to a _specific_ partition, pass the `partition` parameter:

```ruby
# Will write to partition 42.
kafka.deliver_message("Hello, World!", topic: "greetings", partition: 42)
```

If you don't know exactly how many partitions are in the topic, or if you'd rather have some level of indirection, you can pass in `partition_key` instead. Two messages with the same partition key will always be assigned to the same partition. This is useful if you want to make sure all messages with a given attribute are always written to the same partition, e.g. all purchase events for a given customer id.

```ruby
# Partition keys assign a partition deterministically.
kafka.deliver_message("Hello, World!", topic: "greetings", partition_key: "hello")
```

Kafka also supports _message keys_. When passed, a message key can be used instead of a partition key. The message key is written alongside the message value and can be read by consumers. Message keys in Kafka can be used for interesting things such as [Log Compaction](http://kafka.apache.org/documentation.html#compaction). See [Partitioning](#partitioning) for more information.

```ruby
# Set a message key; the key will be used for partitioning since no explicit
# `partition_key` is set.
kafka.deliver_message("Hello, World!", key: "hello", topic: "greetings")
```


#### Efficiently Producing Messages

While `#deliver_message` works fine for infrequent writes, there are a number of downsides:

* Kafka is optimized for transmitting messages in _batches_ rather than individually, so there's a significant overhead and performance penalty in using the single-message API.
* The message delivery can fail in a number of different ways, but this simplistic API does not provide automatic retries.
* The message is not buffered, so if there is an error, it is lost.

The Producer API solves all these problems and more:

```ruby
# Instantiate a new producer.
producer = kafka.producer

# Add a message to the producer buffer.
producer.produce("hello1", topic: "test-messages")

# Deliver the messages to Kafka.
producer.deliver_messages
```

`#produce` will buffer the message in the producer but will _not_ actually send it to the Kafka cluster. Buffered messages are only delivered to the Kafka cluster once `#deliver_messages` is called. Since messages may be destined for different partitions, this could involve writing to more than one Kafka broker. Note that a failure to send all buffered messages after the configured number of retries will result in `Kafka::DeliveryFailed` being raised. This can be rescued and ignored; the messages will be kept in the buffer until the next attempt.

Read the docs for [Kafka::Producer](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Producer) for more details.

#### Asynchronously Producing Messages

A normal producer will block while `#deliver_messages` is sending messages to Kafka, possibly for tens of seconds or even minutes at a time, depending on your timeout and retry settings. Furthermore, you have to call `#deliver_messages` manually, with a frequency that balances batch size with message delay.

In order to avoid blocking during message deliveries you can use the _asynchronous producer_ API. It is mostly similar to the synchronous API, with calls to `#produce` and `#deliver_messages`. The main difference is that rather than blocking, these calls will return immediately. The actual work will be done in a background thread, with the messages and operations being sent from the caller over a thread safe queue.

```ruby
# `#async_producer` will create a new asynchronous producer.
producer = kafka.async_producer

# The `#produce` API works as normal.
producer.produce("hello", topic: "greetings")

# `#deliver_messages` will return immediately.
producer.deliver_messages

# Make sure to call `#shutdown` on the producer in order to avoid leaking
# resources. `#shutdown` will wait for any pending messages to be delivered
# before returning.
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

When calling `#shutdown`, the producer will attempt to deliver the messages and the method call will block until that has happened. Note that there's no _guarantee_ that the messages will be delivered.

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

There's also an example of [encoding messages with Apache Avro](https://github.com/zendesk/ruby-kafka/wiki/Encoding-messages-with-Avro).

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

Another option is to configure a custom client partitioner that implements `call(partition_count, message)` and uses the same schema as the other client. For example:

```ruby
class CustomPartitioner
  def call(partition_count, message)
    ...
  end
end
  
partitioner = CustomPartitioner.new
Kafka.new(partitioner: partitioner, ...)
```

Or, simply create a Proc handling the partitioning logic instead of having to add a new class. For example:

```ruby
partitioner = -> (partition_count, message) { ... }
Kafka.new(partitioner: partitioner, ...)
```

##### Supported partitioning schemes

In order for semantic partitioning to work a `partition_key` must map to the same partition number every time. The general approach, and the one used by this library, is to hash the key and mod it by the number of partitions. There are many different algorithms that can be used to calculate a hash. By default `crc32` is used. `murmur2` is also supported for compatibility with Java based Kafka producers.

To use `murmur2` hashing pass it as an argument to `Partitioner`. For example:

```ruby
Kafka.new(partitioner: Kafka::Partitioner.new(hash_function: :murmur2))
```

#### Buffering and Error Handling

The producer is designed for resilience in the face of temporary network errors, Kafka broker failovers, and other issues that prevent the client from writing messages to the destination topics. It does this by employing local, in-memory buffers. Only when messages are acknowledged by a Kafka broker will they be removed from the buffer.

Typically, you'd configure the producer to retry failed attempts at sending messages, but sometimes all retries are exhausted. In that case, `Kafka::DeliveryFailed` is raised from `Kafka::Producer#deliver_messages`. If you wish to have your application be resilient to this happening (e.g. if you're logging to Kafka from a web application) you can rescue this exception. The failed messages are still retained in the buffer, so a subsequent call to `#deliver_messages` will still attempt to send them.

Note that there's a maximum buffer size; by default, it's set to 1,000 messages and 10MB. It's possible to configure both these numbers:

```ruby
producer = kafka.producer(
  max_buffer_size: 5_000,           # Allow at most 5K messages to be buffered.
  max_buffer_bytesize: 100_000_000, # Allow at most 100MB to be buffered.
  ...
)
```

A final note on buffers: local buffers give resilience against broker and network failures, and allow higher throughput due to message batching, but they also trade off consistency guarantees for higher availability and resilience. If your local process dies while messages are buffered, those messages will be lost. If you require high levels of consistency, you should call `#deliver_messages` immediately after `#produce`.

#### Message Durability

Once the client has delivered a set of messages to a Kafka broker the broker will forward them to its replicas, thus ensuring that a single broker failure will not result in message loss. However, the client can choose _when the leader acknowledges the write_. At one extreme, the client can choose fire-and-forget delivery, not even bothering to check whether the messages have been acknowledged. At the other end, the client can ask the broker to wait until _all_ its replicas have acknowledged the write before returning. This is the safest option, and the default. It's also possible to have the broker return as soon as it has written the messages to its own log but before the replicas have done so. This leaves a window of time where a failure of the leader will result in the messages being lost, although this should not be a common occurrence.

Write latency and throughput are negatively impacted by having more replicas acknowledge a write, so if you require low-latency, high throughput writes you may want to accept lower durability.

This behavior is controlled by the `required_acks` option to `#producer` and `#async_producer`:

```ruby
# This is the default: all replicas must acknowledge.
producer = kafka.producer(required_acks: :all)

# This is fire-and-forget: messages can easily be lost.
producer = kafka.producer(required_acks: 0)

# This only waits for the leader to acknowledge.
producer = kafka.producer(required_acks: 1)
```

Unless you absolutely need lower latency it's highly recommended to use the default setting (`:all`).


#### Message Delivery Guarantees

There are basically two different and incompatible guarantees that can be made in a message delivery system such as Kafka:

1. _at-most-once_ delivery guarantees that a message is at most delivered to the recipient _once_. This is useful only if delivering the message twice carries some risk and should be avoided. Implicit is the fact that there's no guarantee that the message will be delivered at all.
2. _at-least-once_ delivery guarantees that a message is delivered, but it may be delivered more than once. If the final recipient de-duplicates messages, e.g. by checking a unique message id, then it's even possible to implement _exactly-once_ delivery.

Of these two options, ruby-kafka implements the second one: when in doubt about whether a message has been delivered, a producer will try to deliver it again.

The guarantee is made only for the synchronous producer and boils down to this:

```ruby
producer = kafka.producer

producer.produce("hello", topic: "greetings")

# If this line fails with Kafka::DeliveryFailed we *may* have succeeded in delivering
# the message to Kafka but won't know for sure.
producer.deliver_messages

# If we get to this line we can be sure that the message has been delivered to Kafka!
```

That is, once `#deliver_messages` returns we can be sure that Kafka has received the message. Note that there are some big caveats here:

- Depending on how your cluster and topic is configured the message could still be lost by Kafka.
- If you configure the producer to not require acknowledgements from the Kafka brokers by setting `required_acks` to zero there is no guarantee that the message will ever make it to a Kafka broker.
- If you use the asynchronous producer there's no guarantee that messages will have been delivered after `#deliver_messages` returns. A way of blocking until a message has been delivered with the asynchronous producer may be implemented in the future.

It's possible to improve your chances of success when calling `#deliver_messages`, at the price of a longer max latency:

```ruby
producer = kafka.producer(
  # The number of retries when attempting to deliver messages. The default is
  # 2, so 3 attempts in total, but you can configure a higher or lower number:
  max_retries: 5,

  # The number of seconds to wait between retries. In order to handle longer
  # periods of Kafka being unavailable, increase this number. The default is
  # 1 second.
  retry_backoff: 5,
)
```

Note that these values affect the max latency of the operation; see [Understanding Timeouts](#understanding-timeouts) for an explanation of the various timeouts and latencies.

If you use the asynchronous producer you typically don't have to worry too much about this, as retries will be done in the background.

#### Compression

Depending on what kind of data you produce, enabling compression may yield improved bandwidth and space usage. Compression in Kafka is done on entire messages sets rather than on individual messages. This improves the compression rate and generally means that compressions works better the larger your buffers get, since the message sets will be larger by the time they're compressed.

Since many workloads have variations in throughput and distribution across partitions, it's possible to configure a threshold for when to enable compression by setting `compression_threshold`. Only if the defined number of messages are buffered for a partition will the messages be compressed.

Compression is enabled by passing the `compression_codec` parameter to `#producer` with the name of one of the algorithms allowed by Kafka:

* `:snappy` for [Snappy](http://google.github.io/snappy/) compression.
* `:gzip` for [gzip](https://en.wikipedia.org/wiki/Gzip) compression.
* `:lz4` for [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression.
* `:zstd` for [zstd](https://facebook.github.io/zstd/) compression.

By default, all message sets will be compressed if you specify a compression codec. To increase the compression threshold, set `compression_threshold` to an integer value higher than one.

```ruby
producer = kafka.producer(
  compression_codec: :snappy,
  compression_threshold: 10,
)
```

#### Producing Messages from a Rails Application

A typical use case for Kafka is tracking events that occur in web applications. Oftentimes it's advisable to avoid having a hard dependency on Kafka being available, allowing your application to survive a Kafka outage. By using an asynchronous producer, you can avoid doing IO within the individual request/response cycles, instead pushing that to the producer's internal background thread.

In this example, a producer is configured in a Rails initializer:

```ruby
# config/initializers/kafka_producer.rb
require "kafka"

# Configure the Kafka client with the broker hosts and the Rails
# logger.
$kafka = Kafka.new(["kafka1:9092", "kafka2:9092"], logger: Rails.logger)

# Set up an asynchronous producer that delivers its buffered messages
# every ten seconds:
$kafka_producer = $kafka.async_producer(
  delivery_interval: 10,
)

# Make sure to shut down the producer when exiting.
at_exit { $kafka_producer.shutdown }
```

In your controllers, simply call the producer directly:

```ruby
# app/controllers/orders_controller.rb
class OrdersController
  def create
    @order = Order.create!(params[:order])

    event = {
      order_id: @order.id,
      amount: @order.amount,
      timestamp: Time.now,
    }

    $kafka_producer.produce(event.to_json, topic: "order_events")
  end
end
```

### Consuming Messages from Kafka

**Note:** If you're just looking to get started with Kafka consumers, you might be interested in visiting the [Higher level libraries](#higher-level-libraries) section that lists ruby-kafka based frameworks. Read on, if you're interested in either rolling your own executable consumers or if you want to learn more about how consumers work in Kafka.

Consuming messages from a Kafka topic with ruby-kafka is simple:

```ruby
require "kafka"

kafka = Kafka.new(["kafka1:9092", "kafka2:9092"])

kafka.each_message(topic: "greetings") do |message|
  puts message.offset, message.key, message.value
end
```

While this is great for extremely simple use cases, there are a number of downsides:

- You can only fetch from a single topic at a time.
- If you want to have multiple processes consume from the same topic, there's no way of coordinating which processes should fetch from which partitions.
- If the process dies, there's no way to have another process resume fetching from the point in the partition that the original process had reached.


#### Consumer Groups

The Consumer API solves all of the above issues, and more. It uses the Consumer Groups feature released in Kafka 0.9 to allow multiple consumer processes to coordinate access to a topic, assigning each partition to a single consumer. When a consumer fails, the partitions that were assigned to it are re-assigned to other members of the group.

Using the API is simple:

```ruby
require "kafka"

kafka = Kafka.new(["kafka1:9092", "kafka2:9092"])

# Consumers with the same group id will form a Consumer Group together.
consumer = kafka.consumer(group_id: "my-consumer")

# It's possible to subscribe to multiple topics by calling `subscribe`
# repeatedly.
consumer.subscribe("greetings")

# Stop the consumer when the SIGTERM signal is sent to the process.
# It's better to shut down gracefully than to kill the process.
trap("TERM") { consumer.stop }

# This will loop indefinitely, yielding each message in turn.
consumer.each_message do |message|
  puts message.topic, message.partition
  puts message.offset, message.key, message.value
end
```

Each consumer process will be assigned one or more partitions from each topic that the group subscribes to. In order to handle more messages, simply start more processes.

#### Consumer Checkpointing

In order to be able to resume processing after a consumer crashes, each consumer will periodically _checkpoint_ its position within each partition it reads from. Since each partition has a monotonically increasing sequence of message offsets, this works by _committing_ the offset of the last message that was processed in a given partition. Kafka handles these commits and allows another consumer in a group to resume from the last commit when a member crashes or becomes unresponsive.

By default, offsets are committed every 10 seconds. You can increase the frequency, known as the _offset commit interval_, to limit the duration of double-processing scenarios, at the cost of a lower throughput due to the added coordination. If you want to improve throughput, and double-processing is of less concern to you, then you can decrease the frequency. Set the commit interval to zero in order to disable the timer-based commit trigger entirely.

In addition to the time based trigger it's possible to trigger checkpointing in response to _n_ messages having been processed, known as the _offset commit threshold_. This puts a bound on the number of messages that can be double-processed before the problem is detected. Setting this to 1 will cause an offset commit to take place every time a message has been processed. By default this trigger is disabled (set to zero).

It is possible to trigger an immediate offset commit by calling `Consumer#commit_offsets`. This blocks the caller until the Kafka cluster has acknowledged the commit.

Stale offsets are periodically purged by the broker. The broker setting `offsets.retention.minutes` controls the retention window for committed offsets, and defaults to 1 day. The length of the retention window, known as _offset retention time_, can be changed for the consumer.

Previously committed offsets are re-committed, to reset the retention window, at the first commit and periodically at an interval of half the _offset retention time_.

```ruby
consumer = kafka.consumer(
  group_id: "some-group",

  # Increase offset commit frequency to once every 5 seconds.
  offset_commit_interval: 5,

  # Commit offsets when 100 messages have been processed.
  offset_commit_threshold: 100,

  # Increase the length of time that committed offsets are kept.
  offset_retention_time: 7 * 60 * 60
)
```

For some use cases it may be necessary to control when messages are marked as processed. Note that since only the consumer position within each partition can be saved, marking a message as processed implies that all messages in the partition with a lower offset should also be considered as having been processed.

The method `Consumer#mark_message_as_processed` marks a message (and all those that precede it in a partition) as having been processed. This is an advanced API that you should only use if you know what you're doing.

```ruby
# Manually controlling checkpointing:

# Typically you want to use this API in order to buffer messages until some
# special "commit" message is received, e.g. in order to group together
# transactions consisting of several items.
buffer = []

# Messages will not be marked as processed automatically. If you shut down the
# consumer without calling `#mark_message_as_processed` first, the consumer will
# not resume where you left off!
consumer.each_message(automatically_mark_as_processed: false) do |message|
  # Our messages are JSON with a `type` field and other stuff.
  event = JSON.parse(message.value)

  case event.fetch("type")
  when "add_to_cart"
    buffer << event
  when "complete_purchase"
    # We've received all the messages we need, time to save the transaction.
    save_transaction(buffer)

    # Now we can set the checkpoint by marking the last message as processed.
    consumer.mark_message_as_processed(message)

    # We can optionally trigger an immediate, blocking offset commit in order
    # to minimize the risk of crashing before the automatic triggers have
    # kicked in.
    consumer.commit_offsets

    # Make the buffer ready for the next transaction.
    buffer.clear
  end
end
```


#### Topic Subscriptions

For each topic subscription it's possible to decide whether to consume messages starting at the beginning of the topic or to just consume new messages that are produced to the topic. This policy is configured by setting the `start_from_beginning` argument when calling `#subscribe`:

```ruby
# Consume messages from the very beginning of the topic. This is the default.
consumer.subscribe("users", start_from_beginning: true)

# Only consume new messages.
consumer.subscribe("notifications", start_from_beginning: false)
```

Once the consumer group has checkpointed its progress in the topic's partitions, the consumers will always start from the checkpointed offsets, regardless of `start_from_beginning`. As such, this setting only applies when the consumer initially starts consuming from a topic.


#### Shutting Down a Consumer

In order to shut down a running consumer process cleanly, call `#stop` on it. A common pattern is to trap a process signal and initiate the shutdown from there:

```ruby
consumer = kafka.consumer(...)

# The consumer can be stopped from the command line by executing
# `kill -s TERM <process-id>`.
trap("TERM") { consumer.stop }

consumer.each_message do |message|
  ...
end
```


#### Consuming Messages in Batches

Sometimes it is easier to deal with messages in batches rather than individually. A _batch_ is a sequence of one or more Kafka messages that all belong to the same topic and partition. One common reason to want to use batches is when some external system has a batch or transactional API.

```ruby
# A mock search index that we'll be keeping up to date with new Kafka messages.
index = SearchIndex.new

consumer.subscribe("posts")

consumer.each_batch do |batch|
  puts "Received batch: #{batch.topic}/#{batch.partition}"

  transaction = index.transaction

  batch.messages.each do |message|
    # Let's assume that adding a document is idempotent.
    transaction.add(id: message.key, body: message.value)
  end

  # Once this method returns, the messages have been successfully written to the
  # search index. The consumer will only checkpoint a batch *after* the block
  # has completed without an exception.
  transaction.commit!
end
```

One important thing to note is that the client commits the offset of the batch's messages only after the _entire_ batch has been processed.


#### Balancing Throughput and Latency

There are two performance properties that can at times be at odds: _throughput_ and _latency_. Throughput is the number of messages that can be processed in a given timespan; latency is the time it takes from a message is written to a topic until it has been processed.

In order to optimize for throughput, you want to make sure to fetch as many messages as possible every time you do a round trip to the Kafka cluster. This minimizes network overhead and allows processing data in big chunks.

In order to optimize for low latency, you want to process a message as soon as possible, even if that means fetching a smaller batch of messages.

There are three values that can be tuned in order to balance these two concerns.

* `min_bytes` is the minimum number of bytes to return from a single message fetch. By setting this to a high value you can increase the processing throughput. The default value is one byte.
* `max_wait_time` is the maximum number of seconds to wait before returning data from a single message fetch. By setting this high you also increase the processing throughput – and by setting it low you set a bound on latency. This configuration overrides `min_bytes`, so you'll _always_ get data back within the time specified. The default value is one second. If you want to have at most five seconds of latency, set `max_wait_time` to 5. You should make sure `max_wait_time` * num brokers + `heartbeat_interval` is less than `session_timeout`.
* `max_bytes_per_partition` is the maximum amount of data a broker will return for a single partition when fetching new messages. The default is 1MB, but increasing this number may lead to better throughtput since you'll need to fetch less frequently. Setting it to a lower value is not recommended unless you have so many partitions that it's causing network and latency issues to transfer a fetch response from a broker to a client. Setting the number too high may result in instability, so be careful.

The first two settings can be passed to either `#each_message` or `#each_batch`, e.g.

```ruby
# Waits for data for up to 5 seconds on each broker, preferring to fetch at least 5KB at a time.
# This can wait up to num brokers * 5 seconds.
consumer.each_message(min_bytes: 1024 * 5, max_wait_time: 5) do |message|
  # ...
end
```

The last setting is configured when subscribing to a topic, and can vary between topics:

```ruby
# Fetches up to 5MB per partition at a time for better throughput.
consumer.subscribe("greetings", max_bytes_per_partition: 5 * 1024 * 1024)

consumer.each_message do |message|
  # ...
end
```

#### Customizing Partition Assignment Strategy

In some cases, you might want to assign more partitions to some consumers. For example, in applications inserting some records to a database, the consumers running on hosts nearby the database can process more messages than the consumers running on other hosts.
You can use a custom assignment strategy by passing an object that implements `#call` as the argument `assignment_strategy` like below:

```ruby
class CustomAssignmentStrategy
  def initialize(user_data)
    @user_data = user_data
  end

  # Assign the topic partitions to the group members.
  #
  # @param cluster [Kafka::Cluster]
  # @param members [Hash<String, Kafka::Protocol::JoinGroupResponse::Metadata>] a hash
  #   mapping member ids to metadata
  # @param partitions [Array<Kafka::ConsumerGroup::Assignor::Partition>] a list of
  #   partitions the consumer group processes
  # @return [Hash<String, Array<Kafka::ConsumerGroup::Assignor::Partition>] a hash
  #   mapping member ids to partitions.
  def call(cluster:, members:, partitions:)
    ...
  end
end

strategy = CustomAssignmentStrategy.new("some-host-information")
consumer = kafka.consumer(group_id: "some-group", assignment_strategy: strategy)
```

`members` is a hash mapping member IDs to metadata, and partitions is a list of partitions the consumer group processes. The method `call` must return a hash mapping member IDs to partitions. For example, the following strategy assigns partitions randomly:

```ruby
class RandomAssignmentStrategy
  def call(cluster:, members:, partitions:)
    member_ids = members.keys
    partitions.each_with_object(Hash.new {|h, k| h[k] = [] }) do |partition, partitions_per_member|
      partitions_per_member[member_ids[rand(member_ids.count)]] << partition
    end
  end
end
```

If the strategy needs user data, you should define the method `user_data` that returns user data on each consumer. For example, the following strategy uses the consumers' IP addresses as user data:

```ruby
class NetworkTopologyAssignmentStrategy
  def user_data
    Socket.ip_address_list.find(&:ipv4_private?).ip_address
  end

  def call(cluster:, members:, partitions:)
    # Display the pair of the member ID and IP address
    members.each do |id, metadata|
      puts "#{id}: #{metadata.user_data}"
    end

    # Assign partitions considering the network topology
    ...
  end
end
```

Note that the strategy uses the class name as the default protocol name. You can change it by defining the method `protocol_name`:

```ruby
class NetworkTopologyAssignmentStrategy
  def protocol_name
    "networktopology"
  end

  def user_data
    Socket.ip_address_list.find(&:ipv4_private?).ip_address
  end

  def call(cluster:, members:, partitions:)
    ...
  end
end
```

As the method `call` might receive different user data from what it expects, you should avoid using the same protocol name as another strategy that uses different user data.


### Thread Safety

You typically don't want to share a Kafka client object between threads, since the network communication is not synchronized. Furthermore, you should avoid using threads in a consumer unless you're very careful about waiting for all work to complete before returning from the `#each_message` or `#each_batch` block. This is because _checkpointing_ assumes that returning from the block means that the messages that have been yielded have been successfully processed.

You should also avoid sharing a synchronous producer between threads, as the internal buffers are not thread safe. However, the _asynchronous_ producer should be safe to use in a multi-threaded environment. This is because producers, when instantiated, get their own copy of any non-thread-safe data such as network sockets. Furthermore, the asynchronous producer has been designed in such a way to only a single background thread operates on this data while any foreground thread with a reference to the producer object can only send messages to that background thread over a safe queue. Therefore it is safe to share an async producer object between many threads.

### Logging

It's a very good idea to configure the Kafka client with a logger. All important operations and errors are logged. When instantiating your client, simply pass in a valid logger:

```ruby
logger = Logger.new("log/kafka.log")
kafka = Kafka.new(logger: logger, ...)
```

By default, nothing is logged.

### Instrumentation

Most operations are instrumented using [Active Support Notifications](http://api.rubyonrails.org/classes/ActiveSupport/Notifications.html). In order to subscribe to notifications, make sure to require the notifications library:

```ruby
require "active_support/notifications"
require "kafka"
```

The notifications are namespaced based on their origin, with separate namespaces for the producer and the consumer.

In order to receive notifications you can either subscribe to individual notification names or use regular expressions to subscribe to entire namespaces. This example will subscribe to _all_ notifications sent by ruby-kafka:

```ruby
ActiveSupport::Notifications.subscribe(/.*\.kafka$/) do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  puts "Received notification `#{event.name}` with payload: #{event.payload.inspect}"
end
```

All notification events have the `client_id` key in the payload, referring to the Kafka client id.

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

#### Consumer Notifications

All notifications have `group_id` in the payload, referring to the Kafka consumer group id.

* `process_message.consumer.kafka` is sent whenever a message is processed by a consumer. It includes the following payload:
  * `value` is the message value.
  * `key` is the message key.
  * `topic` is the topic that the message was consumed from.
  * `partition` is the topic partition that the message was consumed from.
  * `offset` is the message's offset within the topic partition.
  * `offset_lag` is the number of messages within the topic partition that have not yet been consumed.

* `start_process_message.consumer.kafka` is sent before `process_message.consumer.kafka`, and contains the same payload. It is delivered _before_ the message is processed, rather than _after_.

* `process_batch.consumer.kafka` is sent whenever a message batch is processed by a consumer. It includes the following payload:
  * `message_count` is the number of messages in the batch.
  * `topic` is the topic that the message batch was consumed from.
  * `partition` is the topic partition that the message batch was consumed from.
  * `highwater_mark_offset` is the message batch's highest offset within the topic partition.
  * `offset_lag` is the number of messages within the topic partition that have not yet been consumed.

* `start_process_batch.consumer.kafka` is sent before `process_batch.consumer.kafka`, and contains the same payload. It is delivered _before_ the batch is processed, rather than _after_.

* `join_group.consumer.kafka` is sent whenever a consumer joins a consumer group. It includes the following payload:
  * `group_id` is the consumer group id.

* `sync_group.consumer.kafka` is sent whenever a consumer is assigned topic partitions within a consumer group. It includes the following payload:
  * `group_id` is the consumer group id.

* `leave_group.consumer.kafka` is sent whenever a consumer leaves a consumer group. It includes the following payload:
  * `group_id` is the consumer group id.

* `seek.consumer.kafka` is sent when a consumer first seeks to an offset. It includes the following payload:
  * `group_id` is the consumer group id.
  * `topic` is the topic we are seeking in.
  * `partition` is the partition we are seeking in.
  * `offset` is the offset we have seeked to.

* `heartbeat.consumer.kafka` is sent when a consumer group completes a heartbeat. It includes the following payload:
  * `group_id` is the consumer group id.
  * `topic_partitions` is a hash of { topic_name => array of assigned partition IDs }

#### Connection Notifications

* `request.connection.kafka` is sent whenever a network request is sent to a Kafka broker. It includes the following payload:
  * `api` is the name of the API that was called, e.g. `produce` or `fetch`.
  * `request_size` is the number of bytes in the request.
  * `response_size` is the number of bytes in the response.


### Monitoring

It is highly recommended that you monitor your Kafka client applications in production. Typical problems you'll see are:

* high network error rates, which may impact performance and time-to-delivery;
* producer buffer growth, which may indicate that producers are unable to deliver messages at the rate they're being produced;
* consumer processing errors, indicating exceptions are being raised in the processing code;
* frequent consumer rebalances, which may indicate unstable network conditions or consumer configurations.

You can quite easily build monitoring on top of the provided [instrumentation hooks](#instrumentation). In order to further help with monitoring, a prebuilt [Statsd](https://github.com/etsy/statsd) and [Datadog](https://www.datadoghq.com/) reporter is included with ruby-kafka.


#### What to Monitor

We recommend monitoring the following:

* Low-level Kafka API calls:
    * The rate of API call errors to the total number of calls by both API and broker.
    * The API call throughput by both API and broker.
    * The API call latency by both API and broker.
* Producer-level metrics:
    * Delivery throughput by topic.
    * The latency of deliveries.
    * The producer buffer fill ratios.
    * The async producer queue sizes.
    * Message delivery delays.
    * Failed delivery attempts.
* Consumer-level metrics:
    * Message processing throughput by topic.
    * Processing latency by topic.
    * Processing errors by topic.
    * Consumer lag (how many messages are yet to be processed) by topic/partition.
    * Group join/sync/leave by client host.


#### Reporting Metrics to Statsd

The Statsd reporter is automatically enabled when the `kafka/statsd` library is required. You can optionally change the configuration.

```ruby
require "kafka/statsd"

# Default is "ruby_kafka".
Kafka::Statsd.namespace = "custom-namespace"

# Default is "127.0.0.1".
Kafka::Statsd.host = "statsd.something.com"

# Default is 8125.
Kafka::Statsd.port = 1234
```


#### Reporting Metrics to Datadog

The Datadog reporter is automatically enabled when the `kafka/datadog` library is required. You can optionally change the configuration.

```ruby
# This enables the reporter:
require "kafka/datadog"

# Default is "ruby_kafka".
Kafka::Datadog.namespace = "custom-namespace"

# Default is "127.0.0.1".
Kafka::Datadog.host = "statsd.something.com"

# Default is 8125.
Kafka::Datadog.port = 1234
```

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

### Security

#### Encryption and Authentication using SSL

By default, communication between Kafka clients and brokers is unencrypted and unauthenticated. Kafka 0.9 added optional support for [encryption and client authentication and authorization](http://kafka.apache.org/documentation.html#security_ssl). There are two layers of security made possible by this:

##### Encryption of Communication

By enabling SSL encryption you can have some confidence that messages can be sent to Kafka over an untrusted network without being intercepted.

In this case you just need to pass a valid CA certificate as a string when configuring your `Kafka` client:

```ruby
kafka = Kafka.new(["kafka1:9092"], ssl_ca_cert: File.read('my_ca_cert.pem'))
```

Without passing the CA certificate to the client it would be impossible to protect against [man-in-the-middle attacks](https://en.wikipedia.org/wiki/Man-in-the-middle_attack).

##### Using your system's CA cert store

If you want to use the CA certs from your system's default certificate store, you
can use:

```ruby
kafka = Kafka.new(["kafka1:9092"], ssl_ca_certs_from_system: true)
```

This configures the store to look up CA certificates from the system default certificate store on an as needed basis. The location of the store can usually be determined by:
`OpenSSL::X509::DEFAULT_CERT_FILE`

##### Client Authentication

In order to authenticate the client to the cluster, you need to pass in a certificate and key created for the client and trusted by the brokers.

**NOTE**: You can disable hostname validation by passing `ssl_verify_hostname: false`.

```ruby
kafka = Kafka.new(
  ["kafka1:9092"],
  ssl_ca_cert: File.read('my_ca_cert.pem'),
  ssl_client_cert: File.read('my_client_cert.pem'),
  ssl_client_cert_key: File.read('my_client_cert_key.pem'),
  ssl_client_cert_key_password: 'my_client_cert_key_password',
  ssl_verify_hostname: false,
  # ...
)
```

Once client authentication is set up, it is possible to configure the Kafka cluster to [authorize client requests](http://kafka.apache.org/documentation.html#security_authz).

##### Using JKS Certificates

Typically, Kafka certificates come in the JKS format, which isn't supported by ruby-kafka. There's [a wiki page](https://github.com/zendesk/ruby-kafka/wiki/Creating-X509-certificates-from-JKS-format) that describes how to generate valid X509 certificates from JKS certificates.

#### Authentication using SASL

Kafka has support for using SASL to authenticate clients. Currently GSSAPI, SCRAM and PLAIN mechanisms are supported by ruby-kafka.

**NOTE:** With SASL for authentication, it is highly recommended to use SSL encryption. The default behavior of ruby-kafka enforces you to use SSL and you need to configure SSL encryption by passing `ssl_ca_cert` or enabling `ssl_ca_certs_from_system`. However, this strict SSL mode check can be disabled by setting  `sasl_over_ssl` to `false` while initializing the client.

##### GSSAPI
In order to authenticate using GSSAPI, set your principal and optionally your keytab when initializing the Kafka client:

```ruby
kafka = Kafka.new(
  ["kafka1:9092"],
  sasl_gssapi_principal: 'kafka/kafka.example.com@EXAMPLE.COM',
  sasl_gssapi_keytab: '/etc/keytabs/kafka.keytab',
  # ...
)
```

##### AWS MSK (IAM)
In order to authenticate using IAM w/ an AWS MSK cluster, set your access key, secret key, and region when initializing the Kafka client:

```ruby
k = Kafka.new(
  ["kafka1:9092"],
  sasl_aws_msk_iam_access_key_id: 'iam_access_key',
  sasl_aws_msk_iam_secret_key_id: 'iam_secret_key',
  sasl_aws_msk_iam_aws_region: 'us-west-2',
  ssl_ca_certs_from_system: true,
  # ...
)
```

##### PLAIN
In order to authenticate using PLAIN, you must set your username and password when initializing the Kafka client:

```ruby
kafka = Kafka.new(
  ["kafka1:9092"],
  ssl_ca_cert: File.read('/etc/openssl/cert.pem'),
  sasl_plain_username: 'username',
  sasl_plain_password: 'password'
  # ...
)
```

##### SCRAM
Since 0.11 kafka supports [SCRAM](https://kafka.apache.org/documentation.html#security_sasl_scram).

```ruby
kafka = Kafka.new(
  ["kafka1:9092"],
  sasl_scram_username: 'username',
  sasl_scram_password: 'password',
  sasl_scram_mechanism: 'sha256',
  # ...
)
```

##### OAUTHBEARER
This mechanism is supported in kafka >= 2.0.0 as of [KIP-255](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876)

In order to authenticate using OAUTHBEARER, you must set the client with an instance of a class that implements a `token` method (the interface is described in [Kafka::Sasl::OAuth](lib/kafka/sasl/oauth.rb)) which returns an ID/Access token.

Optionally, the client may implement an `extensions` method that returns a map of key-value pairs. These can be sent with the SASL/OAUTHBEARER initial client response. This is only supported in kafka >= 2.1.0.

```ruby
class TokenProvider
  def token
    "some_id_token"
  end
end
# ...
client = Kafka.new(
  ["kafka1:9092"],
  sasl_oauth_token_provider: TokenProvider.new
)
```

### Topic management

In addition to producing and consuming messages, ruby-kafka supports managing Kafka topics and their configurations. See [the Kafka documentation](https://kafka.apache.org/documentation/#topicconfigs) for a full list of topic configuration keys.

#### List all topics

Return an array of topic names.

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.topics
# => ["topic1", "topic2", "topic3"]
```

#### Create a topic

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.create_topic("topic")
```

By default, the new topic has 1 partition, replication factor 1 and default configs from the brokers. Those configurations are customizable:

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.create_topic("topic",
  num_partitions: 3,
  replication_factor: 2,
  config: {
    "max.message.bytes" => 100000
  }
)
```

#### Create more partitions for a topic

After a topic is created, you can increase the number of partitions for the topic. The new number of partitions must be greater than the current one.

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.create_partitions_for("topic", num_partitions: 10)
```

#### Fetch configuration for a topic (alpha feature)

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.describe_topic("topic", ["max.message.bytes", "retention.ms"])
# => {"max.message.bytes"=>"100000", "retention.ms"=>"604800000"}
```

#### Alter a topic configuration (alpha feature)

Update the topic configurations.

**NOTE**: This feature is for advanced usage. Only use this if you know what you're doing.

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.alter_topic("topic", "max.message.bytes" => 100000, "retention.ms" => 604800000)
```

#### Delete a topic

```ruby
kafka = Kafka.new(["kafka:9092"])
kafka.delete_topic("topic")
```

After a topic is marked as deleted, Kafka only hides it from clients. It would take a while before a topic is completely deleted.

## Design

The library has been designed as a layered system, with each layer having a clear responsibility:

* The **network layer** handles low-level connection tasks, such as keeping open connections to each Kafka broker, reconnecting when there's an error, etc. See [`Kafka::Connection`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Connection) for more details.
* The **protocol layer** is responsible for encoding and decoding the Kafka protocol's various structures. See [`Kafka::Protocol`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Protocol) for more details.
* The **operational layer** provides high-level operations, such as fetching messages from a topic, that may involve more than one API request to the Kafka cluster. Some complex operations are made available through [`Kafka::Cluster`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Cluster), which represents an entire cluster, while simpler ones are only available through [`Kafka::Broker`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Broker), which represents a single Kafka broker. In general, `Kafka::Cluster` is the high-level API, with more polish.
* The **API layer** provides APIs to users of the libraries. The Consumer API is implemented in [`Kafka::Consumer`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Consumer) while the Producer API is implemented in [`Kafka::Producer`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Producer) and [`Kafka::AsyncProducer`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/AsyncProducer).
* The **configuration layer** provides a way to set up and configure the client, as well as easy entrypoints to the various APIs. [`Kafka::Client`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/Client) implements the public APIs. For convenience, the method [`Kafka.new`](http://www.rubydoc.info/gems/ruby-kafka/Kafka.new) can instantiate the class for you.

Note that only the API and configuration layers have any backwards compatibility guarantees – the other layers are considered internal and may change without warning. Don't use them directly.

### Producer Design

The producer is designed with resilience and operational ease of use in mind, sometimes at the cost of raw performance. For instance, the operation is heavily instrumented, allowing operators to monitor the producer at a very granular level.

The producer has two main internal data structures: a list of _pending messages_ and a _message buffer_. When the user calls [`Kafka::Producer#produce`](http://www.rubydoc.info/gems/ruby-kafka/Kafka%2FProducer%3Aproduce), a message is appended to the pending message list, but no network communication takes place. This means that the call site does not have to handle the broad range of errors that can happen at the network or protocol level. Instead, those errors will only happen once [`Kafka::Producer#deliver_messages`](http://www.rubydoc.info/gems/ruby-kafka/Kafka%2FProducer%3Adeliver_messages) is called. This method will go through the pending messages one by one, making sure they're assigned a partition. This may fail for some messages, as it could require knowing the current configuration for the message's topic, necessitating API calls to Kafka. Messages that cannot be assigned a partition are kept in the list, while the others are written into the message buffer. The producer then figures out which topic partitions are led by which Kafka brokers so that messages can be sent to the right place – in Kafka, it is the responsibility of the client to do this routing. A separate _produce_ API request will be sent to each broker; the response will be inspected; and messages that were acknowledged by the broker will be removed from the message buffer. Any messages that were _not_ acknowledged will be kept in the buffer.

If there are any messages left in either the pending message list _or_ the message buffer after this operation, [`Kafka::DeliveryFailed`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/DeliveryFailed) will be raised. This exception must be rescued and handled by the user, possibly by calling `#deliver_messages` at a later time.

### Asynchronous Producer Design

The synchronous producer allows the user fine-grained control over when network activity and the possible errors arising from that will take place, but it requires the user to handle the errors nonetheless. The async producer provides a more hands-off approach that trades off control for ease of use and resilience.

Instead of writing directly into the pending message list, [`Kafka::AsyncProducer`](http://www.rubydoc.info/gems/ruby-kafka/Kafka/AsyncProducer) writes the message to an internal thread-safe queue, returning immediately. A background thread reads messages off the queue and passes them to a synchronous producer.

Rather than triggering message deliveries directly, users of the async producer will typically set up _automatic triggers_, such as a timer.

### Consumer Design

The Consumer API is designed for flexibility and stability. The first is accomplished by not dictating any high-level object model, instead opting for a simple loop-based approach. The second is accomplished by handling group membership, heartbeats, and checkpointing automatically. Messages are marked as processed as soon as they've been successfully yielded to the user-supplied processing block, minimizing the cost of processing errors.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

**Note:** the specs require a working [Docker](https://www.docker.com/) instance, but should work out of the box if you have Docker installed. Please create an issue if that's not the case.

If you would like to contribute to ruby-kafka, please [join our Slack team](https://ruby-kafka-slack.herokuapp.com/) and ask how best to do it.

[![Circle CI](https://circleci.com/gh/zendesk/ruby-kafka.svg?style=shield)](https://circleci.com/gh/zendesk/ruby-kafka/tree/master)

## Support and Discussion

If you've discovered a bug, please file a [Github issue](https://github.com/zendesk/ruby-kafka/issues/new), and make sure to include all the relevant information, including the version of ruby-kafka and Kafka that you're using.

If you have other questions, or would like to discuss best practises, how to contribute to the project, or any other ruby-kafka related topic, [join our Slack team](https://ruby-kafka-slack.herokuapp.com/)!

## Roadmap

Version 0.4 will be the last minor release with support for the Kafka 0.9 protocol. It is recommended that you pin your dependency on ruby-kafka to `~> 0.4.0` in order to receive bugfixes and security updates. New features will only target version 0.5 and up, which will be incompatible with the Kafka 0.9 protocol.

### v0.4

Last stable release with support for the Kafka 0.9 protocol. Bug and security fixes will be released in patch updates.

### v0.5

Latest stable release, with native support for the Kafka 0.10 protocol and eventually newer protocol versions. Kafka 0.9 is no longer supported by this release series.

## Higher level libraries

Currently, there are three actively developed frameworks based on ruby-kafka, that provide higher level API that can be used to work with Kafka messages and two libraries for publishing messages.

### Message processing frameworks

* [Racecar](https://github.com/zendesk/racecar) - A simple framework that integrates with Ruby on Rails to provide a seamless way to write, test, configure, and run Kafka consumers. It comes with sensible defaults and conventions.

* [Karafka](https://github.com/karafka/karafka) - Framework used to simplify Apache Kafka based Ruby and Rails applications development. Karafka provides higher abstraction layers, including Capistrano, Docker and Heroku support.

* [Phobos](https://github.com/klarna/phobos) - Micro framework and library for applications dealing with Apache Kafka. It wraps common behaviors needed by consumers and producers in an easy and convenient API.

### Message publishing libraries

* [DeliveryBoy](https://github.com/zendesk/delivery_boy) – A library that integrates with Ruby on Rails, making it easy to publish Kafka messages from any Rails application.

* [WaterDrop](https://github.com/karafka/waterdrop) – A library for Ruby and Ruby on Rails applications, to easy publish Kafka messages in both sync and async way.

## Why Create A New Library?

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
