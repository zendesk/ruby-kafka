# frozen_string_literal: true

describe "Compression", functional: true do
  let(:topic) { create_random_topic(num_partitions: 3) }

  Kafka::Compression.codecs.each do |codec_name|
    example "producing and consuming #{codec_name}-compressed messages" do
      codec = Kafka::Compression.find_codec(codec_name)
      unless kafka.supports_api?(Kafka::Protocol::PRODUCE_API, codec.produce_api_min_version)
        skip("This Kafka version does not support #{codec_name}")
      end

      producer = kafka.producer(
        compression_codec: codec_name,
        max_retries: 0,
        retry_backoff: 0
      )

      producer.produce("message1", topic: topic, partition: 0)
      producer.produce("message2", topic: topic, partition: 0)

      producer.deliver_messages

      messages = kafka.fetch_messages(
        topic: topic,
        partition: 0,
        offset: 0,
      )

      expect(messages.last(2).map(&:value)).to eq ["message1", "message2"]
    end
  end
end
