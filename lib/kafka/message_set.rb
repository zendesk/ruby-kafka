require "kafka/protocol/message"

module Kafka
  class MessageSet
    def initialize(messages)
      @messages = messages
    end

    def to_h
      hsh = {}
      
      @messages.each do |message|
        value, key = message.value, message.key
        topic, partition = message.topic, message.partition

        hsh[topic] ||= {}
        hsh[topic][partition] ||= []
        hsh[topic][partition] << Protocol::Message.new(value: value, key: key)
      end

      hsh
    end
  end
end
