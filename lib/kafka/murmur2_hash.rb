# frozen_string_literal: true

module Kafka
  class Murmur2Hash
    SEED = [0x9747b28c].pack('L')

    def load
      require 'digest/murmurhash'
    rescue LoadError
      raise LoadError, "using murmur2 hashing requires adding a dependency on the `digest-murmurhash` gem to your Gemfile."
    end

    def hash(value)
      ::Digest::MurmurHash2.rawdigest(value, SEED) & 0x7fffffff
    end
  end
end
