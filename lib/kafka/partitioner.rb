# frozen_string_literal: true

require "kafka/digest"

module Kafka

  # Assigns partitions to messages.
  class Partitioner
    # @param hash_function [Symbol, nil] the algorithm used to compute a messages
    #   destination partition. Default is :crc32
    def initialize(hash_function: nil)
      @digest = Digest.find_digest(hash_function || :crc32)
    end

    # Assigns a partition number based on a partition key. If no explicit
    # partition key is provided, the message key will be used instead.
    #
    # If the key is nil, then a random partition is selected. Otherwise, a digest
    # of the key is used to deterministically find a partition. As long as the
    # number of partitions doesn't change, the same key will always be assigned
    # to the same partition.
    #
    # @param partition_count [Integer] the number of partitions in the topic.
    # @param message [Kafka::PendingMessage] the message that should be assigned
    #   a partition.
    # @return [Integer] the partition number.
    def call(partition_count, message)
      raise ArgumentError if partition_count == 0

      # If no explicit partition key is specified we use the message key instead.
      key = message.partition_key || message.key

      if key.nil?
        rand(partition_count)
      else
        @digest.hash(key) % partition_count
      end
    end
  end
end
