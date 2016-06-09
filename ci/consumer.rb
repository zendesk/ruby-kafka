# Consumes messages from a Kafka topic.

require_relative "init"

consumer = $kafka.consumer(group_id: "greetings-group")
consumer.subscribe("greetings")

num_messages = 0

trap("TERM") { consumer.stop }

consumer.each_message do |message|
  num_messages += 1

  if num_messages % 1000 == 0
    puts "Processed #{num_messages} messages"
  end
end
