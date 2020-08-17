# frozen_string_literal: true

module Kafka
  module Protocol
    class JoinGroupResponse
      Metadata = Struct.new(:version, :topics, :user_data)

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
          members: Hash[
            decoder.array do
              member_id = decoder.string
              d = Decoder.from_string(decoder.bytes)
              [member_id, Metadata.new(d.int16, d.array { d.string }, d.bytes)]
            end
          ],
        )
      end
    end
  end
end
