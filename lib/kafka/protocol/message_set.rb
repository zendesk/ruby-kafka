module Kafka
  module Protocol
    class MessageSet
      attr_reader :messages

      def initialize(messages: [], compression_codec: nil)
        @messages = messages
        @compression_codec = compression_codec
      end

      def ==(other)
        messages == other.messages
      end

      def encode(encoder)
        if @compression_codec.nil?
          encode_without_compression(encoder)
        else
          encode_with_compression(encoder)
        end
      end

      def self.decode(decoder)
        fetched_messages = []

        until decoder.eof?
          message = Message.decode(decoder)

          if message.compressed?
            wrapped_message_set = message.decompress
            fetched_messages.concat(wrapped_message_set.messages)
          else
            fetched_messages << message
          end
        end

        new(messages: fetched_messages)
      end

      private

      def encode_with_compression(encoder)
        codec = @compression_codec

        buffer = StringIO.new
        encode_without_compression(Encoder.new(buffer))
        data = codec.compress(buffer.string)

        wrapper_message = Protocol::Message.new(
          value: data,
          attributes: codec.codec_id,
        )

        message_set = MessageSet.new(messages: [wrapper_message])
        message_set.encode(encoder)
      end

      def encode_without_compression(encoder)
        # Messages in a message set are *not* encoded as an array. Rather,
        # they are written in sequence.
        @messages.each do |message|
          message.encode(encoder)
        end
      end
    end
  end
end
