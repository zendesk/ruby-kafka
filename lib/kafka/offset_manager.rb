module Kafka
  class OffsetManager
    def initialize(group:, logger:, commit_interval:, commit_threshold:)
      @group = group
      @logger = logger
      @commit_interval = commit_interval
      @commit_threshold = commit_threshold

      @uncommitted_offsets = 0
      @processed_offsets = {}
      @default_offsets = {}
      @committed_offsets = nil
      @last_commit = Time.at(0)
    end

    def set_default_offset(topic, default_offset)
      @default_offsets[topic] = default_offset
    end

    def mark_as_processed(topic, partition, offset)
      @uncommitted_offsets += 1
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
      unless @processed_offsets.empty?
        @logger.info "Committing offsets for #{@uncommitted_offsets} messages"

        @group.commit_offsets(@processed_offsets)

        @last_commit = Time.now

        @uncommitted_offsets = 0
        @committed_offsets = nil
      end
    end

    def commit_offsets_if_necessary
      if seconds_since_last_commit >= @commit_interval || commit_threshold_reached?
        commit_offsets
      end
    end

    def clear_offsets
      @uncommitted_offsets = 0
      @processed_offsets.clear
      @committed_offsets = nil
    end

    private

    def seconds_since_last_commit
      Time.now - @last_commit
    end

    def committed_offset_for(topic, partition)
      @committed_offsets ||= @group.fetch_offsets
      @committed_offsets.offset_for(topic, partition)
    end

    def commit_threshold_reached?
      @commit_threshold != 0 && @uncommitted_offsets >= @commit_threshold
    end
  end
end
