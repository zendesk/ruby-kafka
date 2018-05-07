# frozen_string_literal: true

module Kafka
  module Protocol
    class ListGroupsResponse
      class GroupEntry
        attr_reader :group_id, :protocol_type

        def initialize(group_id:, protocol_type:)
          @group_id = group_id
          @protocol_type = protocol_type
        end
      end

      attr_reader :error_code, :groups

      def initialize(error_code:, groups:)
        @error_code = error_code
        @groups = groups
      end

      def self.decode(decoder)
        error_code = decoder.int16
        groups = decoder.array do
          GroupEntry.new(
            group_id: decoder.string,
            protocol_type: decoder.string
          )
        end

        new(error_code: error_code, groups: groups)
      end
    end
  end
end
