# frozen_string_literal: true

describe Kafka::FetchOffsetResolver do
  let(:broker) { double(:broker) }
  let(:logger) { LOGGER }
  let(:resolver) { described_class.new(logger: logger) }

  let(:topics) do
    {
      'hello' => {
        0 => {
          fetch_offset: -1,
          max_bytes: 10_000
        },
        1 => {
          fetch_offset: -2,
          max_bytes: 10_000
        },
        2 => {
          fetch_offset: 33,
          max_bytes: 10_000
        }
      },
      'hi' => {
        2 => {
          fetch_offset: 44,
          max_bytes: 10_000
        },
        3 => {
          fetch_offset: -1,
          max_bytes: 10_000
        }
      },
      'bye' => {
        0 => {
          fetch_offset: 66,
          max_bytes: 10_000
        },
        1 => {
          fetch_offset: 77,
          max_bytes: 10_000
        }
      }
    }
  end

  before do
    allow(broker).to receive(:list_offsets).and_return(
      Kafka::Protocol::ListOffsetResponse.new(
        topics: [
          Kafka::Protocol::ListOffsetResponse::TopicOffsetInfo.new(
            name: 'hello',
            partition_offsets: [
              Kafka::Protocol::ListOffsetResponse::PartitionOffsetInfo.new(
                partition: 0,
                error_code: 0,
                offsets: [11],
              ),
              Kafka::Protocol::ListOffsetResponse::PartitionOffsetInfo.new(
                partition: 1,
                error_code: 0,
                offsets: [22],
              )
            ]
          ),
          Kafka::Protocol::ListOffsetResponse::TopicOffsetInfo.new(
            name: 'hi',
            partition_offsets: [
              Kafka::Protocol::ListOffsetResponse::PartitionOffsetInfo.new(
                partition: 3,
                error_code: 0,
                offsets: [55],
              )
            ]
          )
        ]
      )
    )
  end

  it 'calls broker to update list of partition offsets' do
    expect(broker).to receive(:list_offsets).with(
      topics: {
        'hello' => [
          {
            partition: 0,
            time: -1,
            max_offsets: 1
          },
          {
            partition: 1,
            time: -2,
            max_offsets: 1
          }
        ],
        'hi' => [
          {
            partition: 3,
            time: -1,
            max_offsets: 1
          }
        ]
      }
    )
    resolver.resolve!(broker, topics)
    expect(topics).to eql(
      'hello' => {
        0 => {
          fetch_offset: 11,
          max_bytes: 10_000
        },
        1 => {
          fetch_offset: 22,
          max_bytes: 10_000
        },
        2 => {
          fetch_offset: 33,
          max_bytes: 10_000
        }
      },
      'hi' => {
        2 => {
          fetch_offset: 44,
          max_bytes: 10_000
        },
        3 => {
          fetch_offset: 55,
          max_bytes: 10_000
        }
      },
      'bye' => {
        0 => {
          fetch_offset: 66,
          max_bytes: 10_000
        },
        1 => {
          fetch_offset: 77,
          max_bytes: 10_000
        }
      }
    )
  end
end
