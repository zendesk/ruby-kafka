# frozen_string_literal: true

describe Kafka::FetchedBatch do
  describe "#offset_lag" do
    context "empty batch" do
      it "is 0" do
        fetched_batch = described_class.new(
          topic: 'foo',
          partition: 0,
          highwater_mark_offset: 10,
          messages: []
        )
        expect(fetched_batch.offset_lag).to eq 0
      end
    end

    context "batch contains last message in partition" do
      let(:message) { double(:message, offset: 9) }
      it "is 0" do
        fetched_batch = described_class.new(
          topic: 'foo',
          partition: 0,
          last_offset: 9,
          highwater_mark_offset: 10, # offset of *next* message
          messages: [message]
        )
        expect(fetched_batch.offset_lag).to eq 0
      end
    end

    context "batch does not contain last message in partition" do
      let(:message) { double(:message, offset: 9) }
      it "is the difference between last offset in batch and partition" do
        fetched_batch = described_class.new(
          topic: 'foo',
          partition: 0,
          last_offset: 9,
          highwater_mark_offset: 12, # offset of *next* message
          messages: [message]
        )
        expect(fetched_batch.offset_lag).to eq 2
      end
    end
  end
end
