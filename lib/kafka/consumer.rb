require "kafka/consumer_group"
require "kafka/fetch_operation"

module Kafka
  class Consumer
    def initialize(cluster:, logger:, group_id:, session_timeout: 30)
      @cluster = cluster
      @logger = logger
      @group_id = group_id
      @session_timeout = session_timeout

      @group = ConsumerGroup.new(
        cluster: cluster,
        logger: logger,
        group_id: group_id,
        session_timeout: @session_timeout,
      )

      @offsets = {}
      @default_offset = :earliest
    end

    def subscribe(topic)
      @group.subscribe(topic)
    end

    def each_message(&block)
      loop do
        begin
          batch = fetch_batch
          last_heartbeat = Time.now

          batch.each do |message|
            if last_heartbeat <= Time.now - @session_timeout + 2
              # Make sure we're not kicked out of the group.
              @group.heartbeat
              last_heartbeat = Time.now
            end

            yield message
            mark_message_as_processed(message)
          end
        rescue ConnectionError => e
          @logger.error "Connection error while fetching messages: #{e}"
        else
          commit_offsets unless batch.nil? || batch.empty?
        end
      end
    end

    def mark_message_as_processed(message)
      @offsets[message.topic] ||= {}
      @offsets[message.topic][message.partition] = message.offset + 1
    end

    def fetch_batch
      @group.join unless @group.member?

      @logger.debug "Fetching a batch of messages"

      assigned_partitions = @group.assigned_partitions

      # Make sure we're not kicked out of the group.
      @group.heartbeat

      raise "No partitions assigned!" if assigned_partitions.empty?

      operation = FetchOperation.new(
        cluster: @cluster,
        logger: @logger,
        min_bytes: 1,
        max_wait_time: 5,
      )

      offset_response = @group.fetch_offsets

      assigned_partitions.each do |topic, partitions|
        partitions.each do |partition|
          offset = @offsets.fetch(topic, {}).fetch(partition) {
            offset_response.offset_for(topic, partition)
          }

          offset = @default_offset if offset < 0

          @logger.debug "Fetching from #{topic}/#{partition} starting at offset #{offset}"

          operation.fetch_from_partition(topic, partition, offset: offset)
        end
      end

      messages = operation.execute

      @logger.debug "Fetched #{messages.count} messages"

      messages
    end

    def commit_offsets
      @logger.debug "Committing offsets"
      @group.commit_offsets(@offsets)
    end

    def shutdown
      @group.leave
    end
  end
end
