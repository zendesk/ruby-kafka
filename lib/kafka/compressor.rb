require "kafka/compression"

module Kafka

  # Compresses message sets using a specified codec.
  #
  # A message set is only compressed if its size meets the defined threshold.
  class Compressor

    # @param codec_name [Symbol, nil]
    # @param threshold [Integer] the minimum number of messages in a message set
    #   that will trigger compression.
    def initialize(codec_name:, threshold:)
      @codec = Compression.find_codec(codec_name)
      @threshold = threshold
    end

    # @param message_set [Protocol::MessageSet]
    # @return [Protocol::MessageSet]
    def compress(message_set)
      return message_set if @codec.nil? || message_set.size < @threshold

      data = Protocol::Encoder.encode_with(message_set)
      compressed_data = @codec.compress(data)

      wrapper_message = Protocol::Message.new(
        value: compressed_data,
        attributes: @codec.codec_id,
      )

      Protocol::MessageSet.new(messages: [wrapper_message])
    end
  end
end
