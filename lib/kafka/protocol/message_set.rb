# frozen_string_literal: true

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
          begin
            message = Message.decode(decoder)

            if message.compressed?
              fetched_messages.concat(message.decompress)
            else
              fetched_messages << message
            end
          rescue EOFError
            if fetched_messages.empty?
              # If the first message in the set is truncated, it's likely because the
              # message is larger than the maximum size that we have asked for.
              raise MessageTooLargeToRead
            else
              # We tried to decode a partial message at the end of the set; just skip it.
            end
          end
        end

        new(messages: fetched_messages)
      end
    end
  end
end
