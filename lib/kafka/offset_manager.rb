module Kafka
  class OffsetManager
    def initialize(group:, logger:, commit_interval: 10)
      @group = group
      @logger = logger
      @commit_interval = commit_interval

      @processed_offsets = {}
      @default_offsets = {}
      @committed_offsets = nil
      @last_commit = Time.at(0)
    end

    def set_default_offset(topic, default_offset)
      @default_offsets[topic] = default_offset
    end

    def mark_as_processed(topic, partition, offset)
      @processed_offsets[topic] ||= {}
      @processed_offsets[topic][partition] = offset + 1
    end

    def next_offset_for(topic, partition)
      offset = @processed_offsets.fetch(topic, {}).fetch(partition) {
        committed_offset_for(topic, partition)
      }

      offset = @default_offsets.fetch(topic) if offset < 0

      offset
    end

    def commit_offsets
      @logger.info "Committing offsets"
      @group.commit_offsets(@processed_offsets)
      @last_commit = Time.now
    end

    def commit_offsets_if_necessary
      if Time.now - @last_commit >= @commit_interval
        commit_offsets
      end
    end

    def clear_offsets
      @processed_offsets.clear
      @committed_offsets = nil
    end

    private

    def committed_offset_for(topic, partition)
      @committed_offsets ||= @group.fetch_offsets
      @committed_offsets.offset_for(topic, partition)
    end
  end
end
