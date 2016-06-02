require "kafka/compression"

module Kafka

  # Compresses message sets using a specified codec.
  #
  # A message set is only compressed if its size meets the defined threshold.
  #
  # ## Instrumentation
  #
  # Whenever a message set is compressed, the notification
  # `compress.compressor.kafka` will be emitted with the following payload:
  #
  # * `message_count` – the number of messages in the message set.
  # * `uncompressed_bytesize` – the byte size of the original data.
  # * `compressed_bytesize` – the byte size of the compressed data.
  #
  class Compressor

    # @param codec_name [Symbol, nil]
    # @param threshold [Integer] the minimum number of messages in a message set
    #   that will trigger compression.
    def initialize(codec_name: nil, threshold: 1, instrumenter:)
      @codec = Compression.find_codec(codec_name)
      @threshold = threshold
      @instrumenter = instrumenter
    end

    # @param message_set [Protocol::MessageSet]
    # @return [Protocol::MessageSet]
    def compress(message_set)
      return message_set if @codec.nil? || message_set.size < @threshold

      compressed_data = compress_data(message_set)

      wrapper_message = Protocol::Message.new(
        value: compressed_data,
        codec_id: @codec.codec_id,
      )

      Protocol::MessageSet.new(messages: [wrapper_message])
    end

    private

    def compress_data(message_set)
      data = Protocol::Encoder.encode_with(message_set)

      @instrumenter.instrument("compress.compressor") do |notification|
        compressed_data = @codec.compress(data)

        notification[:message_count] = message_set.size
        notification[:uncompressed_bytesize] = data.bytesize
        notification[:compressed_bytesize] = compressed_data.bytesize

        compressed_data
      end
    end
  end
end
