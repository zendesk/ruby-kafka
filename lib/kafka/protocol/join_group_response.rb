module Kafka
  module Protocol
    class JoinGroupResponse
      attr_reader :error_code

      attr_reader :generation_id, :group_protocol

      attr_reader :leader_id, :member_id, :members

      def initialize(error_code:, generation_id:, group_protocol:, leader_id:, member_id:, members:)
        @error_code = error_code
        @generation_id = generation_id
        @group_protocol = group_protocol
        @leader_id = leader_id
        @member_id = member_id
        @members = members
      end

      def self.decode(decoder)
        new(
          error_code: decoder.int16,
          generation_id: decoder.int32,
          group_protocol: decoder.string,
          leader_id: decoder.string,
          member_id: decoder.string,
          members: Hash[decoder.array { [decoder.string, decoder.bytes] }],
        )
      end
    end
  end
end
