require "zlib"

module Kafka
  class Partitioner
    def initialize(partitions)
      @partitions = partitions
    end

    def partition_for_key(key)
      Zlib.crc32(key) % @partitions.count
    end
  end
end
