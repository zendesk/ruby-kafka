module Kafka
  module Protocol
    class MessageSet
      attr_reader :messages

      def initialize(messages: [])
        @messages = messages
      end

      def size
        @messages.size
      end

      def ==(other)
        messages == other.messages
      end

      def encode(encoder)
        # Messages in a message set are *not* encoded as an array. Rather,
        # they are written in sequence.
        @messages.each do |message|
          message.encode(encoder)
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
    end
  end
end
