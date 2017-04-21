module Kafka
  class OffsetManager

    # The default broker setting for offsets.retention.minutes is 1440.
    DEFAULT_RETENTION_TIME = 1440 * 60

    attr_reader :commit_enabled

    def initialize(cluster:, group:, logger:, commit_interval:, commit_threshold:, offset_retention_time:, commit_enabled:)
      @cluster = cluster
      @group = group
      @logger = logger
      @commit_interval = commit_interval
      @commit_threshold = commit_threshold
      @commit_enabled = commit_enabled

      @uncommitted_offsets = 0
      @processed_offsets = {}
      @default_offsets = {}
      @committed_offsets = nil
      @resolved_offsets = {}
      @last_commit = Time.now
      @last_recommit = nil
      @recommit_interval = (offset_retention_time || DEFAULT_RETENTION_TIME) / 2
    end

    def set_default_offset(topic, default_offset)
      @default_offsets[topic] = default_offset
    end

    def mark_as_processed(topic, partition, offset)
      @uncommitted_offsets += 1
      @processed_offsets[topic] ||= {}

      # The committed offset should always be the offset of the next message that the
      # application will read, thus adding one to the last message processed
      @processed_offsets[topic][partition] = offset + 1
      @logger.debug "Marking #{topic}/#{partition}:#{offset} as committed"
    end

    def seek_to_default(topic, partition)
      @processed_offsets[topic] ||= {}
      @processed_offsets[topic][partition] = -1
    end

    def next_offset_for(topic, partition)
      offset = @processed_offsets.fetch(topic, {}).fetch(partition) {
        committed_offset_for(topic, partition)
      }

      # A negative offset means that no offset has been committed, so we need to
      # resolve the default offset for the topic.
      if offset < 0
        resolve_offset(topic, partition)
      else
        # The next offset is the last offset.
        offset
      end
    end

    def commit_offsets(recommit = false)
      offsets = offsets_to_commit(recommit)
      unless offsets.empty?
        @logger.info "Committing offsets#{recommit ? ' with recommit' : ''}: #{prettify_offsets(offsets)}"

        @group.commit_offsets(offsets)

        @last_commit = Time.now
        @last_recommit = Time.now if recommit

        @uncommitted_offsets = 0
        @committed_offsets = nil
      end
    end

    def commit_offsets_if_necessary
      return unless commit_enabled
      recommit = recommit_timeout_reached?
      if recommit || commit_timeout_reached? || commit_threshold_reached?
        commit_offsets(recommit)
      end
    end

    def clear_offsets
      @processed_offsets.clear
      @resolved_offsets.clear

      # Clear the cached commits from the brokers.
      @committed_offsets = nil
    end

    def clear_offsets_excluding(excluded)
      # Clear all offsets that aren't in `excluded`.
      @processed_offsets.each do |topic, partitions|
        partitions.keep_if do |partition, _|
          excluded.fetch(topic, []).include?(partition)
        end
      end

      # Clear the cached commits from the brokers.
      @committed_offsets = nil
      @resolved_offsets.clear
    end

    private

    def resolve_offset(topic, partition)
      @resolved_offsets[topic] ||= fetch_resolved_offsets(topic)
      @resolved_offsets[topic].fetch(partition)
    end

    def fetch_resolved_offsets(topic)
      default_offset = @default_offsets.fetch(topic)
      partitions = @group.assigned_partitions.fetch(topic)

      @cluster.resolve_offsets(topic, partitions, default_offset)
    end

    def seconds_since(time)
      Time.now - time
    end

    def seconds_since_last_commit
      seconds_since(@last_commit)
    end

    def committed_offsets
      @committed_offsets ||= @group.fetch_offsets
    end

    def committed_offset_for(topic, partition)
      committed_offsets.offset_for(topic, partition)
    end

    def offsets_to_commit(recommit = false)
      if recommit
        offsets_to_recommit.merge!(@processed_offsets) do |_topic, committed, processed|
          committed.merge!(processed)
        end
      else
        @processed_offsets
      end
    end

    def offsets_to_recommit
      committed_offsets.topics.each_with_object({}) do |(topic, partition_info), offsets|
        topic_offsets = partition_info.keys.each_with_object({}) do |partition, partition_map|
          offset = committed_offsets.offset_for(topic, partition)
          partition_map[partition] = offset unless offset == -1
        end
        offsets[topic] = topic_offsets unless topic_offsets.empty?
      end
    end

    def recommit_timeout_reached?
      @last_recommit.nil? || seconds_since(@last_recommit) >= @recommit_interval
    end

    def commit_timeout_reached?
      @commit_interval != 0 && seconds_since_last_commit >= @commit_interval
    end

    def commit_threshold_reached?
      @commit_threshold != 0 && @uncommitted_offsets >= @commit_threshold
    end

    def prettify_offsets(offsets)
      offsets.flat_map do |topic, partitions|
        partitions.map { |partition, offset| "#{topic}/#{partition}:#{offset}" }
      end.join(', ')
    end
  end
end
