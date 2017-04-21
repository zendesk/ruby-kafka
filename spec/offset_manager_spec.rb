require 'timecop'

describe Kafka::OffsetManager do
  let(:cluster) { double(:cluster) }
  let(:group) { double(:group) }
  let(:logger) { LOGGER }

  let(:offset_manager) {
    Kafka::OffsetManager.new(
      cluster: cluster,
      group: group,
      logger: logger,
      commit_interval: commit_interval,
      commit_threshold: 0,
      offset_retention_time: offset_retention_time,
      commit_enabled: commit_enabled
    )
  }
  let(:offset_retention_time) { nil }
  let(:commit_interval) { 0 }
  let(:commit_enabled) { true }


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
          0 => 43,
          1 => 14,
        }
      }

      expect(group).to have_received(:commit_offsets).with(expected_offsets)
    end
  end

  describe "#commit_offsets_if_necessary" do
    let(:fetched_offsets_response) do
      Kafka::Protocol::OffsetFetchResponse.new(topics: {
        "greetings" => {
          0 => partition_offset_info(-1),
          1 => partition_offset_info(24),
          2 => partition_offset_info(4)
        }
      })
    end

    before do
      allow(group).to receive(:fetch_offsets).and_return(fetched_offsets_response)
    end

    context "at the first commit" do
      it "re-commits previously committed offsets" do
        fetch_committed_offsets
        offset_manager.mark_as_processed("greetings", 1, 25)

        offset_manager.commit_offsets_if_necessary

        expected_offsets = {
          "greetings" => {
            1 => 26,
            2 => 4
          }
        }

        expect(group).to have_received(:commit_offsets).with(expected_offsets)
      end
    end

    context "commit intervals" do
      let(:commit_interval) { 10 }
      let(:offset_retention_time) { 300 }
      let(:commits) { [] }

      before do
        allow(group).to receive(:commit_offsets) do |offsets|
          commits << offsets
        end
        Timecop.freeze(Time.now)
        # initial commit
        fetch_committed_offsets
        offset_manager.mark_as_processed("greetings", 0, 0)
        offset_manager.commit_offsets_if_necessary
        expect(commits.size).to eq(1)
      end

      after do
        Timecop.return
      end

      context "before the commit timeout" do
        before do
          Timecop.travel(commit_interval - 1)
        end

        it "does not commit processed offsets to the group" do
          expect do
            offset_manager.mark_as_processed("greetings", 0, 1)
            offset_manager.commit_offsets_if_necessary
          end.not_to change(commits, :size)
        end
      end

      context "after the commit timeout" do
        before do
          Timecop.travel(commit_interval + 1)
        end

        it "commits processed offsets without recommitting previously committed offsets" do
          expect do
            offset_manager.mark_as_processed("greetings", 0, 1)
            offset_manager.commit_offsets_if_necessary
          end.to change(commits, :size).by(1)

          expected_offsets = {
            "greetings" => { 0 => 2 }
          }

          expect(commits.last).to eq(expected_offsets)
        end
      end

      context "after the recommit timeout" do
        before do
          Timecop.travel(offset_retention_time / 2 + 1)
        end

        it "commits processed offsets and recommits previously committed offsets" do
          expect do
            offset_manager.mark_as_processed("greetings", 0, 1)
            offset_manager.commit_offsets_if_necessary
          end.to change(commits, :size).by(1)

          expected_offsets = {
            "greetings" => {
              0 => 2,
              1 => 24,
              2 => 4
            }
          }

          expect(commits.last).to eq(expected_offsets)
        end
      end
    end

    context "commit disabled" do
      let(:commit_enabled) { false }

      it "it does nothing" do
        expect(offset_manager).not_to receive(:commit_offsets)
        offset_manager.commit_offsets_if_necessary
      end
    end

    def fetch_committed_offsets
      offset_manager.next_offset_for("greetings", 1)
    end

    def partition_offset_info(offset)
      Kafka::Protocol::OffsetFetchResponse::PartitionOffsetInfo.new(offset: offset, metadata: nil, error_code: 0)
    end
  end

  describe "#next_offset_for" do
    let(:fetched_offsets) { double(:fetched_offsets) }

    before do
      allow(group).to receive(:fetch_offsets).and_return(fetched_offsets)
    end

    it "returns the last committed offset" do
      allow(fetched_offsets).to receive(:offset_for).with("greetings", 0) { 41 }

      offset = offset_manager.next_offset_for("greetings", 0)

      expect(offset).to eq 41
    end

    it "returns the default offset if none have been committed" do
      allow(group).to receive(:assigned_partitions) { { "greetings" => [0] } }
      allow(fetched_offsets).to receive(:offset_for).with("greetings", 0) { -1 }
      allow(cluster).to receive(:resolve_offsets).with("greetings", [0], :latest) { { 0 => 42 } }
      offset_manager.set_default_offset("greetings", :latest)

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
          0 => 43,
        }
      }

      expect(group).to have_received(:commit_offsets).with(expected_offsets)
    end
  end
end
