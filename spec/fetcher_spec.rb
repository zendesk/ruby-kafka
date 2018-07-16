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
end
