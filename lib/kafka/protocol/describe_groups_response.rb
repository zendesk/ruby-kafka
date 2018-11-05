# frozen_string_literal: true

module Kafka
  module Protocol
    class DescribeGroupsResponse
      class Group
        attr_reader :error_code, :group_id, :state, :members, :protocol

        def initialize(error_code:, group_id:, protocol_type:, protocol:, state:, members:)
          @error_code = error_code
          @group_id = group_id
          @protocol_type = protocol_type
          @protocol = protocol
          @state = state
          @members = members
        end
      end

      class Member
        attr_reader :member_id, :client_id, :client_host, :member_assignment

        def initialize(member_id:, client_id:, client_host:, member_assignment:)
          @member_id = member_id
          @client_id = client_id
          @client_host = client_host
          @member_assignment = member_assignment
        end
      end

      attr_reader :error_code, :groups

      def initialize(groups:)
        @groups = groups
      end

      def self.decode(decoder)
        groups = decoder.array do
          error_code = decoder.int16
          group_id = decoder.string
          state = decoder.string
          protocol_type = decoder.string
          protocol = decoder.string

          members = decoder.array do
            member_id = decoder.string
            client_id = decoder.string
            client_host = decoder.string
            _metadata = decoder.bytes
            assignment = MemberAssignment.decode(Decoder.from_string(decoder.bytes))

            Member.new(
              member_id: member_id,
              client_id: client_id,
              client_host: client_host,
              member_assignment: assignment
            )
          end

          Group.new(
            error_code: error_code,
            group_id: group_id,
            state: state,
            protocol_type: protocol_type,
            protocol: protocol,
            members: members
          )
        end

        new(groups: groups)
      end
    end
  end
end
