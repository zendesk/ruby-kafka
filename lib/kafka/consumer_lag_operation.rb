module Kafka
    class ConsumerLagOperation
        def initialize(cluster:, group:, logger:)
            @cluster = cluster
            @logger = TaggedLogger.new(logger)
            @group = group
        end

        def execute
            puts 'EXECUTE CONUMER LAG OPERATION'
            begin
                group_offsets = @cluster.fetch_group_offsets(@group)
                topics = group_offsets.keys
                @cluster.add_target_topics(topics)
                @cluster.refresh_metadata_if_necessary!
                topic_offsets = last_offsets_for(topics)
                offsets = {}

                offsets = group_offsets.map {|topic, topic_details|
                    t = topic_details.map {|partition, p|
                        off = {
                            offset: p.offset,
                            topic_offset: topic_offsets[topic][partition],
                            consumer_lag: topic_offsets[topic][partition] - p.offset 
                        }
                        [partition, off]
                    }.to_h
                    [topic, t]
                }.to_h
                puts 'CONSUMER LAG OPERATION FINISHED'
                return [offsets]
            rescue => exception
                puts exception
            end
        end

        def last_offsets_for(topics)
            topics.map {|topic|
                partition_ids = @cluster.partitions_for(topic).collect(&:partition_id)
                partition_offsets = @cluster.resolve_offsets(topic, partition_ids, :latest)
                [topic, partition_offsets.collect { |k, v| [k, v - 1] }.to_h]
            }.to_h
        end

    end
end
# k = Kafka.new(['localhost:9092'])
# k.fetch_consumer_lag(group_id: 'java-consumer')

# Kafka.new(['localhost:9092']).consumer_lag(group_id: 'java-consumer').fetch_lags do |l|
#     puts l.to_s + 'OUTSIDE'
# end

# Kafka.new(['localhost:9092']).fetch_consumer_lag(group_id: 'java-consumer') do |l|
#     puts l
# end

# Kafka.new(['localhost:9092']).consumer_lag(group_id: 'java-consumer').fetch_lags