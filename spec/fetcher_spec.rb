# frozen_string_literal: true

describe Kafka::Fetcher do
  let(:group) { double(:group, :group_id => '123') }
  let(:instrumenter) { double(:instrumenter) }
  let(:logger) { Logger.new(StringIO.new) }
  let(:cluster) { double(:cluster) }
  let(:fetcher) do
    described_class.new(group: group,
                        instrumenter: instrumenter,
                        logger: logger,
                        cluster: cluster,
                        max_queue_size: 1)
  end

  before do
    # don't actually start the loop
    thread = double(:thread)
    allow(thread).to receive(:abort_on_exception=)
    allow(Thread).to receive(:new).and_return(thread)
  end

  describe '#handle_seek' do
    it "should instrument seeks" do
      allow(instrumenter).to receive(:instrument)
      fetcher.send(:handle_seek, 'my-topic', 1, 2)
      expect(instrumenter).to have_received(:instrument).with(
        'seek.consumer',
        group_id: '123',
        topic: 'my-topic',
        partition: 1,
        offset: 2
      )
    end
  end

  describe '#step' do
    it "queues a missing codec as an exception for the consumer instead of raising in the fetcher thread" do
      snappy = Kafka::Compression::CODECS_BY_NAME.fetch(:snappy)
      allow(snappy).to receive(:load).and_raise(LoadError, "snappy gem missing")
      # Decoding a fetched snappy batch looks the codec up by id.
      allow(fetcher).to receive(:fetch_batches) { Kafka::Compression.find_codec_by_id(snappy.codec_id) }

      expect { fetcher.send(:step) }.not_to raise_error

      tag, error = fetcher.poll
      expect(tag).to eq :exception
      expect(error).to be_a(Kafka::CodecUnavailable)
    end
  end
end
