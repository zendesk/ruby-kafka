# Continuously produces messages to a Kafka topic.

require_relative "init"

producer = $kafka.async_producer(
  delivery_interval: 1,
  max_queue_size: 5_000,
  max_buffer_size: 10_000,
)

num_messages = 0
shutdown = false

trap("TERM") { shutdown = true }

until shutdown
  begin
    producer.produce("hello", key: "world", topic: "greetings")
  rescue Kafka::BufferOverflow
    puts "Buffer overflow, backing off..."
    sleep 10
  end
end

producer.shutdown
