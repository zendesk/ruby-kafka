module Kafka
  module Protocol
    class ApiKey
      PRODUCE         = 0
      FETCH           = 1
      OFFSETS         = 2
      METADATA        = 3
      LEADER_AND_ISR  = 4
      STOP_REPLICA    = 5
      UPDATE_METADATA = 6
      CONTROLLED_SHUTDOWN = 7
      OFFSET_COMMIT   = 8
      OFFSET_FETCH    = 9
      GROUP_COORDINATOR = 10
      JOIN_GROUP      = 11
      HEARTBEAT       = 12
      LEAVE_GROUP     = 13
      SYNC_GROUP      = 14
      DESCRIBE_GROUPS = 15
      LIST_GROUPS     = 16
      SASL_HANDSHAKE  = 17
      API_VERSIONS    = 18
      CREATE_TOPICS   = 19
      DELETE_TOPICS   = 20
    end
  end
end
