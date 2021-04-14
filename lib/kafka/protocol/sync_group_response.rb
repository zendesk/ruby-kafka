# frozen_string_literal: true

require "kafka/protocol/member_assignment"

module Kafka
  module Protocol
    class SyncGroupResponse
      attr_reader :error_code, :member_assignment

      def initialize(error_code:, member_assignment:)
        @error_code = error_code
        @member_assignment = member_assignment
      end

      def self.decode(decoder)
        error_code = decoder.int16
        member_assignment_bytes = decoder.bytes

        new(
          error_code: error_code,
          member_assignment: member_assignment_bytes ? MemberAssignment.decode(Decoder.from_string(member_assignment_bytes)) : nil
        )
      end
    end
  end
end
