module Kafka
  PendingMessage = Struct.new(
    "PendingMessage",
    :value,
    :key,
    :topic,
    :partition,
    :partition_key,
    :create_time,
    :bytesize)
end
