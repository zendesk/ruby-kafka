# frozen_string_literal: true

require "kafka/consumer_group"
require "kafka/offset_manager"
require "kafka/fetcher"
require "kafka/pause"

module Kafka

  # A client that consumes messages from a Kafka cluster in coordination with
  # other clients.
  #
  # A Consumer subscribes to one or more Kafka topics; all consumers with the
  # same *group id* then agree on who should read from the individual topic
  # partitions. When group members join or leave, the group synchronizes,
  # making sure that all partitions are assigned to a single member, and that
  # all members have some partitions to read from.
  #
  # ## Example
  #
  # A simple producer that simply writes the messages it consumes to the
  # console.
  #
  #     require "kafka"
  #
  #     kafka = Kafka.new(["kafka1:9092", "kafka2:9092"])
  #
  #     # Create a new Consumer instance in the group `my-group`:
  #     consumer = kafka.consumer(group_id: "my-group")
  #
  #     # Subscribe to a Kafka topic:
  #     consumer.subscribe("messages")
  #
  #     # Loop forever, reading in messages from all topics that have been
  #     # subscribed to.
  #     consumer.each_message do |message|
  #       puts message.topic
  #       puts message.partition
  #       puts message.key
  #       puts message.value
  #       puts message.offset
  #     end
  #
  class Consumer

    def initialize(cluster:, logger:, instrumenter:, group:, fetcher:, offset_manager:, session_timeout:, heartbeat:)
      @cluster = cluster
      @logger = logger
      @instrumenter = instrumenter
      @group = group
      @offset_manager = offset_manager
      @session_timeout = session_timeout
      @fetcher = fetcher
      @heartbeat = heartbeat

      @pauses = Hash.new {|h, k|
        h[k] = Hash.new {|h2, k2|
          h2[k2] = Pause.new
        }
      }

      # Whether or not the consumer is currently consuming messages.
      @running = false

      # Hash containing offsets for each topic and partition that has the
      # automatically_mark_as_processed feature disabled. Offset manager is only active
      # when everything is suppose to happen automatically. Otherwise we need to keep track of the
      # offset manually in memory for all the time
      # The key structure for this equals an array with topic and partition [topic, partition]
      # The value is equal to the offset of the last message we've received
      # @note It won't be updated in case user marks message as processed, because for the case
      #   when user commits message other than last in a batch, this would make ruby-kafka refetch
      #   some already consumed messages
      @current_offsets = Hash.new { |h, k| h[k] = {} }
    end

    # Subscribes the consumer to a topic.
    #
    # Typically you either want to start reading messages from the very
    # beginning of the topic's partitions or you simply want to wait for new
    # messages to be written. In the former case, set `start_from_beginning`
    # to true (the default); in the latter, set it to false.
    #
    # @param topic [String] the name of the topic to subscribe to.
    # @param default_offset [Symbol] whether to start from the beginning or the
    #   end of the topic's partitions. Deprecated.
    # @param start_from_beginning [Boolean] whether to start from the beginning
    #   of the topic or just subscribe to new messages being produced. This
    #   only applies when first consuming a topic partition – once the consumer
    #   has checkpointed its progress, it will always resume from the last
    #   checkpoint.
    # @param max_bytes_per_partition [Integer] the maximum amount of data fetched
    #   from a single partition at a time.
    # @return [nil]
    def subscribe(topic, default_offset: nil, start_from_beginning: true, max_bytes_per_partition: 1048576)
      default_offset ||= start_from_beginning ? :earliest : :latest

      @group.subscribe(topic)
      @offset_manager.set_default_offset(topic, default_offset)
      @fetcher.subscribe(topic, max_bytes_per_partition: max_bytes_per_partition)

      nil
    end

    # Stop the consumer.
    #
    # The consumer will finish any in-progress work and shut down.
    #
    # @return [nil]
    def stop
      @running = false
      @fetcher.stop
      @cluster.disconnect
    end

    # Pause processing of a specific topic partition.
    #
    # When a specific message causes the processor code to fail, it can be a good
    # idea to simply pause the partition until the error can be resolved, allowing
    # the rest of the partitions to continue being processed.
    #
    # If the `timeout` argument is passed, the partition will automatically be
    # resumed when the timeout expires. If `exponential_backoff` is enabled, each
    # subsequent pause will cause the timeout to double until a message from the
    # partition has been successfully processed.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @param timeout [nil, Integer] the number of seconds to pause the partition for,
    #   or `nil` if the partition should not be automatically resumed.
    # @param max_timeout [nil, Integer] the maximum number of seconds to pause for,
    #   or `nil` if no maximum should be enforced.
    # @param exponential_backoff [Boolean] whether to enable exponential backoff.
    # @return [nil]
    def pause(topic, partition, timeout: nil, max_timeout: nil, exponential_backoff: false)
      if max_timeout && !exponential_backoff
        raise ArgumentError, "`max_timeout` only makes sense when `exponential_backoff` is enabled"
      end

      pause_for(topic, partition).pause!(
        timeout: timeout,
        max_timeout: max_timeout,
        exponential_backoff: exponential_backoff,
      )
    end

    # Resume processing of a topic partition.
    #
    # @see #pause
    # @param topic [String]
    # @param partition [Integer]
    # @return [nil]
    def resume(topic, partition)
      pause_for(topic, partition).resume!

      seek_to_next(topic, partition)
    end

    # Whether the topic partition is currently paused.
    #
    # @see #pause
    # @param topic [String]
    # @param partition [Integer]
    # @return [Boolean] true if the partition is paused, false otherwise.
    def paused?(topic, partition)
      pause = pause_for(topic, partition)
      pause.paused? && !pause.expired?
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each message is yielded to the provided block. If the block returns
    # without raising an exception, the message will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from each broker; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_bytes [Integer] the maximum number of bytes to read before
    #   returning messages from each broker.
    # @param max_wait_time [Integer, Float] the maximum duration of time to wait before
    #   returning messages from each broker, in seconds.
    # @param automatically_mark_as_processed [Boolean] whether to automatically
    #   mark a message as successfully processed when the block returns
    #   without an exception. Once marked successful, the offsets of processed
    #   messages can be committed to Kafka.
    # @yieldparam message [Kafka::FetchedMessage] a message fetched from Kafka.
    # @raise [Kafka::ProcessingError] if there was an error processing a message.
    #   The original exception will be returned by calling `#cause` on the
    #   {Kafka::ProcessingError} instance.
    # @return [nil]
    def each_message(min_bytes: 1, max_bytes: 10485760, max_wait_time: 1, automatically_mark_as_processed: true)
      @fetcher.configure(
        min_bytes: min_bytes,
        max_bytes: max_bytes,
        max_wait_time: max_wait_time,
      )

      consumer_loop do
        batches = fetch_batches

        batches.each do |batch|
          batch.messages.each do |message|
            notification = {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              offset_lag: batch.highwater_mark_offset - message.offset - 1,
              create_time: message.create_time,
              key: message.key,
              value: message.value,
            }

            # Instrument an event immediately so that subscribers don't have to wait until
            # the block is completed.
            @instrumenter.instrument("start_process_message.consumer", notification)

            @instrumenter.instrument("process_message.consumer", notification) do
              begin
                yield message
                @current_offsets[message.topic][message.partition] = message.offset
              rescue => e
                location = "#{message.topic}/#{message.partition} at offset #{message.offset}"
                backtrace = e.backtrace.join("\n")
                @logger.error "Exception raised when processing #{location} -- #{e.class}: #{e}\n#{backtrace}"

                raise ProcessingError.new(message.topic, message.partition, message.offset)
              end
            end

            mark_message_as_processed(message) if automatically_mark_as_processed
            @offset_manager.commit_offsets_if_necessary

            trigger_heartbeat

            return if !@running
          end

          # We've successfully processed a batch from the partition, so we can clear
          # the pause.
          pause_for(batch.topic, batch.partition).reset!
        end

        # We may not have received any messages, but it's still a good idea to
        # commit offsets if we've processed messages in the last set of batches.
        # This also ensures the offsets are retained if we haven't read any messages
        # since the offset retention period has elapsed.
        @offset_manager.commit_offsets_if_necessary
      end
    end

    # Fetches and enumerates the messages in the topics that the consumer group
    # subscribes to.
    #
    # Each batch of messages is yielded to the provided block. If the block returns
    # without raising an exception, the batch will be considered successfully
    # processed. At regular intervals the offset of the most recent successfully
    # processed message batch in each partition will be committed to the Kafka
    # offset store. If the consumer crashes or leaves the group, the group member
    # that is tasked with taking over processing of these partitions will resume
    # at the last committed offsets.
    #
    # @param min_bytes [Integer] the minimum number of bytes to read before
    #   returning messages from each broker; if `max_wait_time` is reached, this
    #   is ignored.
    # @param max_bytes [Integer] the maximum number of bytes to read before
    #   returning messages from each broker.
    # @param max_wait_time [Integer, Float] the maximum duration of time to wait before
    #   returning messages from each broker, in seconds.
    # @param automatically_mark_as_processed [Boolean] whether to automatically
    #   mark a batch's messages as successfully processed when the block returns
    #   without an exception. Once marked successful, the offsets of processed
    #   messages can be committed to Kafka.
    # @yieldparam batch [Kafka::FetchedBatch] a message batch fetched from Kafka.
    # @return [nil]
    def each_batch(min_bytes: 1, max_bytes: 10485760, max_wait_time: 1, automatically_mark_as_processed: true)
      @fetcher.configure(
        min_bytes: min_bytes,
        max_bytes: max_bytes,
        max_wait_time: max_wait_time,
      )

      consumer_loop do
        batches = fetch_batches

        batches.each do |batch|
          unless batch.empty?
            notification = {
              topic: batch.topic,
              partition: batch.partition,
              last_offset: batch.last_offset,
              offset_lag: batch.offset_lag,
              highwater_mark_offset: batch.highwater_mark_offset,
              message_count: batch.messages.count,
            }

            # Instrument an event immediately so that subscribers don't have to wait until
            # the block is completed.
            @instrumenter.instrument("start_process_batch.consumer", notification)

            @instrumenter.instrument("process_batch.consumer", notification) do
              begin
                yield batch
                @current_offsets[batch.topic][batch.partition] = batch.last_offset
              rescue => e
                offset_range = (batch.first_offset..batch.last_offset)
                location = "#{batch.topic}/#{batch.partition} in offset range #{offset_range}"
                backtrace = e.backtrace.join("\n")

                @logger.error "Exception raised when processing #{location} -- #{e.class}: #{e}\n#{backtrace}"

                raise ProcessingError.new(batch.topic, batch.partition, offset_range)
              end
            end

            mark_message_as_processed(batch.messages.last) if automatically_mark_as_processed

            # We've successfully processed a batch from the partition, so we can clear
            # the pause.
            pause_for(batch.topic, batch.partition).reset!
          end

          @offset_manager.commit_offsets_if_necessary

          trigger_heartbeat

          return if !@running
        end

        # We may not have received any messages, but it's still a good idea to
        # commit offsets if we've processed messages in the last set of batches.
        # This also ensures the offsets are retained if we haven't read any messages
        # since the offset retention period has elapsed.
        @offset_manager.commit_offsets_if_necessary
      end
    end

    # Move the consumer's position in a topic partition to the specified offset.
    #
    # Note that this has to be done prior to calling {#each_message} or {#each_batch}
    # and only has an effect if the consumer is assigned the partition. Typically,
    # you will want to do this in every consumer group member in order to make sure
    # that the member that's assigned the partition knows where to start.
    #
    # @param topic [String]
    # @param partition [Integer]
    # @param offset [Integer]
    # @return [nil]
    def seek(topic, partition, offset)
      @offset_manager.seek_to(topic, partition, offset)
    end

    def commit_offsets
      @offset_manager.commit_offsets
    end

    def mark_message_as_processed(message)
      @offset_manager.mark_as_processed(message.topic, message.partition, message.offset)
    end

    def trigger_heartbeat
      @heartbeat.trigger
    end

    def trigger_heartbeat!
      @heartbeat.trigger!
    end

    # Aliases for the external API compatibility
    alias send_heartbeat_if_necessary trigger_heartbeat
    alias send_heartbeat trigger_heartbeat!

    private

    def consumer_loop
      @running = true

      @fetcher.start

      while @running
        begin
          @instrumenter.instrument("loop.consumer") do
            yield
          end
        rescue HeartbeatError, OffsetCommitError
          join_group
        rescue RebalanceInProgress
          @logger.warn "Group rebalance in progress, re-joining..."
          join_group
        rescue FetchError, NotLeaderForPartition, UnknownTopicOrPartition
          @cluster.mark_as_stale!
        rescue LeaderNotAvailable => e
          @logger.error "Leader not available; waiting 1s before retrying"
          @cluster.mark_as_stale!
          sleep 1
        rescue ConnectionError => e
          @logger.error "Connection error #{e.class}: #{e.message}"
          @cluster.mark_as_stale!
        rescue SignalException => e
          @logger.warn "Received signal #{e.message}, shutting down"
          @running = false
        end
      end
    ensure
      @fetcher.stop

      # In order to quickly have the consumer group re-balance itself, it's
      # important that members explicitly tell Kafka when they're leaving.
      make_final_offsets_commit!
      @group.leave rescue nil
      @running = false
    end

    def make_final_offsets_commit!(attempts = 3)
      @offset_manager.commit_offsets
    rescue ConnectionError, OffsetCommitError, EOFError
      # It's important to make sure final offsets commit is done
      # As otherwise messages that have been processed after last auto-commit
      # will be processed again and that may be huge amount of messages
      return if attempts.zero?

      @logger.error "Retrying to make final offsets commit (#{attempts} attempts left)"
      sleep(0.1)
      make_final_offsets_commit!(attempts - 1)
    rescue Kafka::Error => e
      @logger.error "Encountered error while shutting down; #{e.class}: #{e.message}"
    end

    def join_group
      old_generation_id = @group.generation_id

      @group.join

      if old_generation_id && @group.generation_id != old_generation_id + 1
        # We've been out of the group for at least an entire generation, no
        # sense in trying to hold on to offset data
        clear_current_offsets
        @offset_manager.clear_offsets
      else
        # After rejoining the group we may have been assigned a new set of
        # partitions. Keeping the old offset commits around forever would risk
        # having the consumer go back and reprocess messages if it's assigned
        # a partition it used to be assigned to way back. For that reason, we
        # only keep commits for the partitions that we're still assigned.
        clear_current_offsets(excluding: @group.assigned_partitions)
        @offset_manager.clear_offsets_excluding(@group.assigned_partitions)
      end

      @fetcher.reset

      @group.assigned_partitions.each do |topic, partitions|
        partitions.each do |partition|
          if paused?(topic, partition)
            @logger.warn "Not fetching from #{topic}/#{partition} due to pause"
          else
            seek_to_next(topic, partition)
          end
        end
      end
    end

    def seek_to_next(topic, partition)
      # When automatic marking is off, the first poll needs to be based on the last committed
      # offset from Kafka, that's why we fallback in case of nil (it may not be 0)
      if @current_offsets[topic].key?(partition)
        offset = @current_offsets[topic][partition] + 1
      else
        offset = @offset_manager.next_offset_for(topic, partition)
      end

      @fetcher.seek(topic, partition, offset)
    end

    def resume_paused_partitions!
      @pauses.each do |topic, partitions|
        partitions.each do |partition, pause|
          @instrumenter.instrument("pause_status.consumer", {
            topic: topic,
            partition: partition,
            duration: pause.pause_duration,
          })

          if pause.paused? && pause.expired?
            @logger.info "Automatically resuming partition #{topic}/#{partition}, pause timeout expired"
            resume(topic, partition)
          end
        end
      end
    end

    def fetch_batches
      # Return early if the consumer has been stopped.
      return [] if !@running

      join_group unless @group.member?

      trigger_heartbeat

      resume_paused_partitions!

      if !@fetcher.data?
        @logger.debug "No batches to process"
        sleep 2
        []
      else
        tag, message = @fetcher.poll

        case tag
        when :batches
          # make sure any old batches, fetched prior to the completion of a consumer group sync,
          # are only processed if the batches are from brokers for which this broker is still responsible.
          message.select { |batch| @group.assigned_to?(batch.topic, batch.partition) }
        when :exception
          raise message
        end
      end
    rescue OffsetOutOfRange => e
      @logger.error "Invalid offset #{e.offset} for #{e.topic}/#{e.partition}, resetting to default offset"

      @offset_manager.seek_to_default(e.topic, e.partition)

      retry
    rescue ConnectionError => e
      @logger.error "Connection error while fetching messages: #{e}"

      raise FetchError, e
    end

    def pause_for(topic, partition)
      @pauses[topic][partition]
    end

    def clear_current_offsets(excluding: {})
      @current_offsets.each do |topic, partitions|
        partitions.keep_if do |partition, _|
          excluding.fetch(topic, []).include?(partition)
        end
      end
    end
  end
end
