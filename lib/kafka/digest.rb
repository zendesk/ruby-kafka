# frozen_string_literal: true

require "kafka/crc32_hash"
require "kafka/murmur2_hash"

module Kafka
  module Digest
    FUNCTIONS_BY_NAME = {
      :crc32 => Crc32Hash.new,
      :murmur2 => Murmur2Hash.new
    }.freeze

    def self.find_digest(name)
      digest = FUNCTIONS_BY_NAME.fetch(name) do
        raise LoadError, "Unknown hash function #{name}"
      end

      digest.load
      digest
    end
  end
end
