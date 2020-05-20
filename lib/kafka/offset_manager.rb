# frozen_string_literal: true

module Kafka

  # Manages a consumer's position in partitions, figures out where to resume processing
  # from, etc.
  class OffsetManager

    # The default broker setting for offsets.retention.minutes is 1440.
    DEFAULT_RETENTION_TIME = 1440 * 60

    def initialize(cluster:, group:, fetcher:, logger:, commit_interval:, commit_threshold:, offset_retention_time:)
      @cluster = cluster
      @group = group
      @fetcher = fetcher
      @logger = TaggedLogger.new(logger)
      @commit_interval = commit_interval
      @commit_threshold = commit_threshold

      @uncommitted_offsets = 0
      @processed_offsets = {}
      @default_offsets = {}
      @committed_offsets = nil
      @resolved_offsets = {}
      @last_commit = Time.now
      @last_recommit = nil
      @recommit_interval = (offset_retention_time || DEFAULT_RETENTION_TIME) / 2
    end

    # Set the default offset for a topic.
    #
    # When the consumer is started for the first time, or in cases where it gets stuck and
    # has to reset its position, it must start either with the earliest messages or with
    # the latest, skipping to the very end of each partition.
    #
    # @param topic [String] the name of the topic.
    # @param default_offset [Symbol] either `:earliest` or `:latest`.
    # @return [nil]
    def set_default_offset(topic, default_offset)
      @default_offsets[topic] = default_offset
    end

    # Mark a message as having been processed.
    #
    # When offsets are committed, the message's offset will be stored in Kafka so
    # that we can resume from this point at a later time.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition number.
    # @param offset [Integer] the offset of the message that should be marked as processed.
    # @return [nil]
    def mark_as_processed(topic, partition, offset)
      unless @group.assigned_to?(topic, partition)
        @logger.debug "Not marking #{topic}/#{partition}:#{offset} as processed for partition not assigned to this consumer."
        return
      end
      @processed_offsets[topic] ||= {}

      last_processed_offset = @processed_offsets[topic][partition] || -1
      if last_processed_offset > offset + 1
        @logger.debug "Not overwriting newer offset #{topic}/#{partition}:#{last_processed_offset - 1} with older #{offset}"
        return
      end

      @uncommitted_offsets += 1

      # The committed offset should always be the offset of the next message that the
      # application will read, thus adding one to the last message processed.
      @processed_offsets[topic][partition] = offset + 1
      @logger.debug "Marking #{topic}/#{partition}:#{offset} as processed"
    end

    # Move the consumer's position in the partition back to the configured default
    # offset, either the first or latest in the partition.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition number.
    # @return [nil]
    def seek_to_default(topic, partition)
      # Remove any cached offset, in case things have changed broker-side.
      clear_resolved_offset(topic)

      offset = resolve_offset(topic, partition)

      seek_to(topic, partition, offset)
    end

    # Move the consumer's position in the partition to the specified offset.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition number.
    # @param offset [Integer] the offset that the consumer position should be moved to.
    # @return [nil]
    def seek_to(topic, partition, offset)
      @processed_offsets[topic] ||= {}
      @processed_offsets[topic][partition] = offset

      @fetcher.seek(topic, partition, offset)
    end

    # Return the next offset that should be fetched for the specified partition.
    #
    # @param topic [String] the name of the topic.
    # @param partition [Integer] the partition number.
    # @return [Integer] the next offset that should be fetched.
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

    # Commit offsets of messages that have been marked as processed.
    #
    # If `recommit` is set to true, we will also commit the existing positions
    # even if no messages have been processed on a partition. This is done
    # in order to avoid the offset information expiring in cases where messages
    # are very rare -- it's essentially a keep-alive.
    #
    # @param recommit [Boolean] whether to recommit offsets that have already been
    #   committed.
    # @return [nil]
    def commit_offsets(recommit = false)
      offsets = offsets_to_commit(recommit)
      unless offsets.empty?
        @logger.debug "Committing offsets#{recommit ? ' with recommit' : ''}: #{prettify_offsets(offsets)}"

        @group.commit_offsets(offsets)

        @last_commit = Time.now
        @last_recommit = Time.now if recommit

        @uncommitted_offsets = 0
        @committed_offsets = nil
      end
    end

    # Commit offsets if necessary, according to the offset commit policy specified
    # when initializing the class.
    #
    # @return [nil]
    def commit_offsets_if_necessary
      recommit = recommit_timeout_reached?
      if recommit || commit_timeout_reached? || commit_threshold_reached?
        commit_offsets(recommit)
      end
    end

    # Clear all stored offset information.
    #
    # @return [nil]
    def clear_offsets
      @processed_offsets.clear
      @resolved_offsets.clear

      # Clear the cached commits from the brokers.
      @committed_offsets = nil
    end

    # Clear stored offset information for all partitions except those specified
    # in `excluded`.
    #
    #     offset_manager.clear_offsets_excluding("my-topic" => [1, 2, 3])
    #
    # @return [nil]
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

    def clear_resolved_offset(topic)
      @resolved_offsets.delete(topic)
    end

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
