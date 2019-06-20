# frozen_string_literal: true

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
    attr_reader :codec

    # @param codec_name [Symbol, nil]
    # @param threshold [Integer] the minimum number of messages in a message set
    #   that will trigger compression.
    def initialize(codec_name: nil, threshold: 1, instrumenter:)
      # Codec may be nil, in which case we won't compress.
      @codec = codec_name && Compression.find_codec(codec_name)

      @threshold = threshold
      @instrumenter = instrumenter
    end

    # @param record_batch [Protocol::RecordBatch]
    # @param offset [Integer] used to simulate broker behaviour in tests
    # @return [Protocol::RecordBatch]
    def compress(record_batch, offset: -1)
      if record_batch.is_a?(Protocol::RecordBatch)
        compress_record_batch(record_batch)
      else
        # Deprecated message set format
        compress_message_set(record_batch, offset)
      end
    end

    private

    def compress_message_set(message_set, offset)
      return message_set if @codec.nil? || message_set.size < @threshold

      data = Protocol::Encoder.encode_with(message_set)
      compressed_data = @codec.compress(data)

      @instrumenter.instrument("compress.compressor") do |notification|
        notification[:message_count] = message_set.size
        notification[:uncompressed_bytesize] = data.bytesize
        notification[:compressed_bytesize] = compressed_data.bytesize
      end

      wrapper_message = Protocol::Message.new(
        value: compressed_data,
        codec_id: @codec.codec_id,
        offset: offset
      )

      Protocol::MessageSet.new(messages: [wrapper_message])
    end

    def compress_record_batch(record_batch)
      if @codec.nil? || record_batch.size < @threshold
        record_batch.codec_id = 0
        return Protocol::Encoder.encode_with(record_batch)
      end

      record_batch.codec_id = @codec.codec_id
      data = Protocol::Encoder.encode_with(record_batch)

      @instrumenter.instrument("compress.compressor") do |notification|
        notification[:message_count] = record_batch.size
        notification[:compressed_bytesize] = data.bytesize
      end

      data
    end
  end
end
