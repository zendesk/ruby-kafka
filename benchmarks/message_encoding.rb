# frozen_string_literal: true

require "kafka"

ready "message serialization" do
  before do
    message = Kafka::Protocol::Message.new(
      value: "hello",
      key: "world",
    )

    @io = StringIO.new
    encoder = Kafka::Protocol::Encoder.new(@io)
    message.encode(encoder)

    @decoder = Kafka::Protocol::Decoder.new(@io)
  end

  go "decoding" do
    @io.rewind
    Kafka::Protocol::Message.decode(@decoder)
  end
end
