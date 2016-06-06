# Producing Messages to Kafka

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