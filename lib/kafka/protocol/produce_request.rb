module Kafka
  module Protocol
    class ProduceRequest
      def initialize(required_acks:, timeout:, messages_for_topics:)
        @required_acks = required_acks
        @timeout = timeout
        @messages_for_topics = messages_for_topics
      end

      def requires_acks?
        @required_acks != 0
      end

      def encode(encoder)
        encoder.write_int16(@required_acks)
        encoder.write_int32(@timeout)

        encoder.write_array(@messages_for_topics) do |topic, messages_for_partition|
          encoder.write_string(topic)

          encoder.write_array(messages_for_partition) do |partition, messages|
            message_set_buffer = StringIO.new
            message_set_encoder = Encoder.new(message_set_buffer)

            # Messages in a message set are *not* encoded as an array. Rather,
            # they are written in sequence with only the byte size prepended.
            messages.each do |message|
              offset = -1 # offsets don't matter here.
              message_buffer = StringIO.new
              message_encoder = Encoder.new(message_buffer)
              message.encode(message_encoder)

              message_set_encoder.write_int64(offset)
              message_set_encoder.write_bytes(message_buffer.string)
            end

            encoder.write_int32(partition)
            encoder.write_bytes(message_set_buffer.string)
          end
        end
      end
    end
  end
end
