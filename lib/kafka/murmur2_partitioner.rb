# frozen_string_literal: true

require 'digest/murmurhash'

module Kafka

  # Java producer compatible message partitioner
  class Murmur2Partitioner
    SEED = [0x9747b28c].pack('L')

    # Assigns a partition number based on a partition key. If no explicit
    # partition key is provided, the message key will be used instead.
    #
    # If the key is nil, then a random partition is selected. Otherwise, a hash
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

      key = message.partition_key || message.key

      if key.nil?
        rand(partition_count)
      else
        (Digest::MurmurHash2.rawdigest(key, SEED) & 0x7fffffff) % partition_count
      end
    end
  end
end
