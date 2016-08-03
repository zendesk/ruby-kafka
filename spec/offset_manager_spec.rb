describe Kafka::OffsetManager do
  let(:cluster) { double(:cluster) }
  let(:group) { double(:group) }
  let(:logger) { LOGGER }

  let(:offset_manager) {
    Kafka::OffsetManager.new(
      cluster: cluster,
      group: group,
      logger: logger,
      commit_interval: 0,
      commit_threshold: 0,
    )
  }

  before do
    allow(group).to receive(:commit_offsets)
  end

  describe "#commit_offsets" do
    it "commits the processed offsets to the group" do
      offset_manager.mark_as_processed("greetings", 0, 42)
      offset_manager.mark_as_processed("greetings", 1, 13)

      offset_manager.commit_offsets

      expected_offsets = {
        "greetings" => {
          0 => 42,
          1 => 13,
        }
      }

      expect(group).to have_received(:commit_offsets).with(expected_offsets)
    end
  end

  describe "#next_offset_for" do
    let(:fetched_offsets) { double(:fetched_offsets) }

    before do
      allow(group).to receive(:fetch_offsets).and_return(fetched_offsets)
    end

    it "returns the last committed offset plus one" do
      allow(fetched_offsets).to receive(:offset_for).with("greetings", 0) { 41 }

      offset = offset_manager.next_offset_for("greetings", 0)

      expect(offset).to eq 42
    end

    it "returns the default offset if none have been committed" do
      allow(group).to receive(:assigned_partitions) { { "greetings" => [0] } }
      allow(fetched_offsets).to receive(:offset_for).with("greetings", 0) { -1 }
      allow(cluster).to receive(:resolve_offsets).with("greetings", [0], :latest) { { 0 => 42 } }
      offset_manager.set_default_offset("greetings", :latest)

      offset = offset_manager.next_offset_for("greetings", 0)

      expect(offset).to eq 42
    end

    it "returns the next offset if we've already processed messages in the partition" do
      offset_manager.mark_as_processed("greetings", 0, 41)

      offset = offset_manager.next_offset_for("greetings", 0)

      expect(offset).to eq 42
    end
  end

  describe "#clear_offsets_excluding" do
    it "clears offsets except for the partitions in the exclusion list" do
      offset_manager.mark_as_processed("x", 0, 42)
      offset_manager.mark_as_processed("x", 1, 13)

      offset_manager.clear_offsets_excluding("x" => [0])
      offset_manager.commit_offsets

      expected_offsets = {
        "x" => {
          0 => 42,
        }
      }

      expect(group).to have_received(:commit_offsets).with(expected_offsets)
    end
  end
end
