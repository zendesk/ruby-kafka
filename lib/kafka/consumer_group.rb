# frozen_string_literal: true

require "set"
require "kafka/consumer_group/assignor"
require "kafka/round_robin_assignment_strategy"

module Kafka
  class ConsumerGroup
    attr_reader :assigned_partitions, :generation_id, :group_id

    def initialize(cluster:, logger:, group_id:, session_timeout:, rebalance_timeout:, retention_time:, instrumenter:, assignment_strategy:)
      @cluster = cluster
      @logger = TaggedLogger.new(logger)
      @group_id = group_id
      @session_timeout = session_timeout
      @rebalance_timeout = rebalance_timeout
      @instrumenter = instrumenter
      @member_id = ""
      @generation_id = nil
      @members = {}
      @topics = Set.new
      @assigned_partitions = {}
      @assignor = Assignor.new(
        cluster: cluster,
        strategy: assignment_strategy || RoundRobinAssignmentStrategy.new
      )
      @retention_time = retention_time
    end

    def subscribe(topic)
      @topics.add(topic)
      @cluster.add_target_topics([topic])
    end

    def subscribed_partitions
      @assigned_partitions.select { |topic, _| @topics.include?(topic) }
    end

    def assigned_to?(topic, partition)
      subscribed_partitions.fetch(topic, []).include?(partition)
    end

    def member?
      !@generation_id.nil?
    end

    def join
      if @topics.empty?
        raise Kafka::Error, "Cannot join group without at least one topic subscription"
      end

      join_group
      synchronize
    rescue NotCoordinatorForGroup
      @logger.error "Failed to find coordinator for group `#{@group_id}`; retrying..."
      sleep 1
      @coordinator = nil
      retry
    rescue ConnectionError
      @logger.error "Connection error while trying to join group `#{@group_id}`; retrying..."
      sleep 1
      @cluster.mark_as_stale!
      @coordinator = nil
      retry
    end

    def leave
      @logger.info "Leaving group `#{@group_id}`"

      # Having a generation id indicates that we're a member of the group.
      @generation_id = nil

      @instrumenter.instrument("leave_group.consumer", group_id: @group_id) do
        coordinator.leave_group(group_id: @group_id, member_id: @member_id)
      end
    rescue ConnectionError
    end

    def fetch_offsets
      coordinator.fetch_offsets(
        group_id: @group_id,
        topics: @assigned_partitions,
      )
    end

    def commit_offsets(offsets)
      response = coordinator.commit_offsets(
        group_id: @group_id,
        member_id: @member_id,
        generation_id: @generation_id,
        offsets: offsets,
        retention_time: @retention_time
      )

      response.topics.each do |topic, partitions|
        partitions.each do |partition, error_code|
          Protocol.handle_error(error_code)
        end
      end
    rescue Kafka::Error => e
      @logger.error "Error committing offsets: #{e}"
      raise OffsetCommitError, e
    end

    def heartbeat
      @logger.debug "Sending heartbeat..."

      @instrumenter.instrument('heartbeat.consumer',
                               group_id: @group_id,
                               topic_partitions: @assigned_partitions) do

        response = coordinator.heartbeat(
          group_id: @group_id,
          generation_id: @generation_id,
          member_id: @member_id,
        )

        Protocol.handle_error(response.error_code)
      end
    rescue ConnectionError, UnknownMemberId, IllegalGeneration => e
      @logger.error "Error sending heartbeat: #{e}"
      raise HeartbeatError, e
    rescue RebalanceInProgress => e
      @logger.warn "Error sending heartbeat: #{e}"
      raise HeartbeatError, e
    rescue NotCoordinatorForGroup
      @logger.error "Failed to find coordinator for group `#{@group_id}`; retrying..."
      sleep 1
      @coordinator = nil
      retry
    end

    def to_s
      "[#{@group_id}] {" + assigned_partitions.map { |topic, partitions|
        partition_str = partitions.size > 5 ?
                          "#{partitions[0..4].join(', ')}..." :
                          partitions.join(', ')
        "#{topic}: #{partition_str}"
      }.join('; ') + '}:'
    end

    private

    def join_group
      @logger.info "Joining group `#{@group_id}`"

      @instrumenter.instrument("join_group.consumer", group_id: @group_id) do
        response = coordinator.join_group(
          group_id: @group_id,
          session_timeout: @session_timeout,
          rebalance_timeout: @rebalance_timeout,
          member_id: @member_id,
          topics: @topics,
          protocol_name: @assignor.protocol_name,
          user_data: @assignor.user_data,
        )

        Protocol.handle_error(response.error_code)

        @generation_id = response.generation_id
        @member_id = response.member_id
        @leader_id = response.leader_id
        @members = response.members
      end

      @logger.info "Joined group `#{@group_id}` with member id `#{@member_id}`"
    rescue UnknownMemberId
      @logger.error "Failed to join group; resetting member id and retrying in 1s..."

      @member_id = ""
      sleep 1

      retry
    rescue CoordinatorLoadInProgress
      @logger.error "Coordinator broker still loading, retrying in 1s..."

      sleep 1

      retry
    end

    def group_leader?
      @member_id == @leader_id
    end

    def synchronize
      group_assignment = {}

      if group_leader?
        @logger.info "Chosen as leader of group `#{@group_id}`"

        topics = Set.new
        @members.each do |_member, metadata|
          metadata.topics.each { |t| topics.add(t) }
        end

        group_assignment = @assignor.assign(
          members: @members,
          topics: topics,
        )
      end

      @instrumenter.instrument("sync_group.consumer", group_id: @group_id) do
        response = coordinator.sync_group(
          group_id: @group_id,
          generation_id: @generation_id,
          member_id: @member_id,
          group_assignment: group_assignment,
        )

        Protocol.handle_error(response.error_code)

        response.member_assignment.topics.each do |topic, assigned_partitions|
          @logger.info "Partitions assigned for `#{topic}`: #{assigned_partitions.join(', ')}"
        end

        @assigned_partitions.replace(response.member_assignment.topics)
      end
    end

    def coordinator
      @coordinator ||= @cluster.get_group_coordinator(group_id: @group_id)
    rescue CoordinatorNotAvailable
      @logger.error "Group coordinator not available for group `#{@group_id}`"

      sleep 1

      retry
    end
  end
end
