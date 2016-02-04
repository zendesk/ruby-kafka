module Kafka
  module Protocol
    class MessageSet
      attr_reader :messages

      def initialize(messages:)
        @messages = messages
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
