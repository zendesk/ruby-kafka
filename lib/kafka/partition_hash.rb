module Kafka
  class PartitionHash
    include Enumerable

    EMPTY = {}

    def initialize
      @topics = {}
    end

    def [](topic, partition)
      @topics.fetch(topic, EMPTY).fetch(partition)
    end

    def []=(topic, partition, value)
      @topics[topic] ||= {}
      @topics[topic][partition] = value
    end

    def empty?
      @topics.empty?
    end

    def each
      @topics.each do |topic, partitions|
        partitions.each do |partition, value|
          yield topic, partition, value
        end
      end
    end

    def keep_if
      @topics.keep_if do |topic, partitions|
        partitions.keep_if do |partition, value|
          yield topic, partition, value
        end

        !partitions.empty?
      end
    end

    def fetch(topic, partition, &fallback)
      @topics.fetch(topic, EMPTY).fetch(partition, &fallback)
    end

    def to_h
      @topics
    end
  end
end
