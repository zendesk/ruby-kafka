# frozen_string_literal: true

require "zlib"

module Kafka
  class Crc32Hash

    # crc32 is supported natively
    def load; end

    def hash(value)
      Zlib.crc32(value)
    end
  end
end
