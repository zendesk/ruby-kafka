# Configuring the Kafka Client

A client must be initialized with at least one Kafka broker, from which the entire Kafka cluster will be discovered. Each client keeps a separate pool of broker connections. Don't use the same client from more than one thread.

```ruby
require "kafka"

kafka = Kafka.new(
  # At least one of these nodes must be available:
  seed_brokers: ["kafka1:9092", "kafka2:9092"],
  
  # Set an optional client id in order to identify the client to Kafka:
  client_id: "my-application",
)
```