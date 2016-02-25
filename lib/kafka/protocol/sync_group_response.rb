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
        new(
          error_code: decoder.int16,
          member_assignment: MemberAssignment.decode(Decoder.from_string(decoder.bytes)),
        )
      end
    end
  end
end
