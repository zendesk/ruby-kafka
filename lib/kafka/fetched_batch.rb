module Kafka
  class FetchedBatch
    attr_reader :topic, :partition, :highwater_mark_offset, :messages

    def initialize(topic:, partition:, highwater_mark_offset:, messages:)
      @topic = topic
      @partition = partition
      @highwater_mark_offset = highwater_mark_offset
      @messages = messages
    end
  end
end
