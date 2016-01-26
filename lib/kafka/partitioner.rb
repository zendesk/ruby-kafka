require "zlib"

module Kafka

  # Assigns partitions to messages.
  class Partitioner
    def initialize(partitions)
      @partitions = partitions
    end

    # Assigns a partition number based on a key.
    #
    # If the key is nil, then a random partition is selected. Otherwise, a digest
    # of the key is used to deterministically find a partition. As long as the
    # number of partitions doesn't change, the same key will always be assigned
    # to the same partition.
    #
    # @param key [String, nil] the key to base the partition assignment on, or nil.
    # @return [Integer] the partition number.
    def partition_for_key(key)
      if key.nil?
        rand(@partitions.count)
      else
        Zlib.crc32(key) % @partitions.count
      end
    end
  end
end
