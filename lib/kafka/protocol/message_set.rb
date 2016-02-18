module Kafka
  module Protocol
    class MessageSet
      attr_reader :messages

      def initialize(messages:)
        @messages = messages
      end

      def encode(encoder)
        # Messages in a message set are *not* encoded as an array. Rather,
        # they are written in sequence.
        @messages.each do |message|
          offset = -1 # offsets don't matter here.
          encoder.write_int64(offset)

          # When encoding a message into a message set, the bytesize of the message must
          # precede the actual bytes. Therefore we need to encode the message into a
          # separate buffer first.
          encoded_message = Encoder.encode_with(message)
          encoder.write_bytes(encoded_message)
        end
      end

      def self.decode(decoder)
        fetched_messages = []

        until decoder.eof?
          offset = decoder.int64
          message_decoder = Decoder.from_string(decoder.bytes)
          message = Message.decode(message_decoder)

          fetched_messages << [offset, message]
        end

        new(messages: fetched_messages)
      end
    end
  end
end
