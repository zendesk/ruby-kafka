require "set"
require "kafka/round_robin_assignment_strategy"

module Kafka
  class ConsumerGroup
    attr_reader :assigned_partitions

    def initialize(cluster:, logger:, group_id:)
      @cluster = cluster
      @logger = logger
      @group_id = group_id
      @session_timeout = 30
      @member_id = ""
      @generation_id = nil
      @members = {}
      @topics = Set.new
      @assigned_partitions = {}
      @assignment_strategy = RoundRobinAssignmentStrategy.new(cluster: @cluster)
    end

    def subscribe(topic)
      @topics.add(topic)
      @cluster.add_target_topics([topic])
    end

    def member?
      !@generation_id.nil?
    end

    def join
      join_group
      synchronize
    rescue NotCoordinatorForGroup
      @coordinator = nil
      retry
    rescue ConnectionError => e
      @logger.error "Connection error (#{e}), retrying..."
      @coordinator = nil
      retry
    end

    def leave
      @logger.info "[#{@member_id}] Leaving group `#{@group_id}`"
      coordinator.leave_group(group_id: @group_id, member_id: @member_id)
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
      )

      response.topics.each do |topic, partitions|
        partitions.each do |partition, error_code|
          Protocol.handle_error(error_code)
        end
      end
    rescue UnknownMemberId
      @logger.error "Kicked out of group; rejoining"
      join
      retry
    rescue IllegalGeneration
      @logger.error "Illegal generation #{@generation_id}; rejoining group"
      join
      retry
    end

    def heartbeat
      @logger.info "[#{@member_id}] Sending heartbeat..."

      response = coordinator.heartbeat(
        group_id: @group_id,
        generation_id: @generation_id,
        member_id: @member_id,
      )

      Protocol.handle_error(response.error_code)
    rescue ConnectionError => e
      @logger.error "Connection error while sending heartbeat; rejoining"
      join
    rescue UnknownMemberId
      @logger.error "Kicked out of group; rejoining"
      join
    rescue RebalanceInProgress
      @logger.error "Group is rebalancing; rejoining"
      join
    end

    private

    def join_group
      @logger.info "Joining group `#{@group_id}`"

      response = coordinator.join_group(
        group_id: @group_id,
        session_timeout: @session_timeout,
        member_id: @member_id,
      )

      Protocol.handle_error(response.error_code)

      @generation_id = response.generation_id
      @member_id = response.member_id
      @leader_id = response.leader_id
      @members = response.members

      @logger.info "[#{@member_id}] Joined group `#{@group_id}` with member id `#{@member_id}`"
      @logger.info "[#{@member_id}] Leader for group `#{@group_id}` is `#{response.leader_id}`"
    rescue UnknownMemberId
      @member_id = nil

      retry
    end

    def group_leader?
      @member_id == @leader_id
    end

    def synchronize
      @logger.info "[#{@member_id}] Synchronizing group"

      group_assignment = {}

      if group_leader?
        @logger.info "[#{@member_id}] Chosen as leader of group `#{@group_id}`"

        group_assignment = @assignment_strategy.assign(
          members: @members.keys,
          topics: @topics,
        )
      end

      response = coordinator.sync_group(
        group_id: @group_id,
        generation_id: @generation_id,
        member_id: @member_id,
        group_assignment: group_assignment,
      )

      Protocol.handle_error(response.error_code)

      response.member_assignment.topics.each do |topic, assigned_partitions|
        @logger.info "[#{@member_id}] Partitions assigned for `#{topic}`: #{assigned_partitions.join(', ')}"
      end

      @assigned_partitions.replace(response.member_assignment.topics)
    end

    def coordinator
      @coordinator ||= @cluster.get_group_coordinator(group_id: @group_id)
    rescue GroupCoordinatorNotAvailable
      @logger.error "Group coordinator not available for group `#{@group_id}`"

      sleep 1

      retry
    end
  end
end
