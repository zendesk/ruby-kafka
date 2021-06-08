# frozen_string_literal: true

describe Kafka::RoundRobinAssignmentStrategy do
  let(:strategy) { described_class.new }

  it "assigns all partitions" do
    members = Hash[(0...10).map {|i| ["member#{i}", double(topics: ['greetings'])] }]
    partitions = (0...30).map {|i| double(:"partition#{i}", topic: "greetings", partition_id: i) }

    assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end
  end

  it "spreads all partitions between members" do
    topics = ["topic1", "topic2"]
    members = Hash[(0...10).map {|i| ["member#{i}", double(topics: topics)] }]
    partitions = topics.product((0...5).to_a).map {|topic, i|
      double(:"partition#{i}", topic: topic, partition_id: i)
    }

    assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end

    num_partitions_assigned = assignments.values.map do |assigned_partitions|
      assigned_partitions.count
    end

    expect(num_partitions_assigned).to all eq(1)
  end

  Metadata = Struct.new(:topics)
  [
    {
      name: "uneven topics",
      topics: { "topic1" => [0], "topic2" => (0..50).to_a },
      members: {
        "member1" => Metadata.new(["topic1", "topic2"]),
        "member2" => Metadata.new(["topic1", "topic2"])
      },
    },
    {
      name: "only one partition",
      topics: { "topic1" => [0] },
      members: {
        "member1" => Metadata.new(["topic1"]),
        "member2" => Metadata.new(["topic1"])
      },
    },
    {
      name: "lots of partitions",
      topics: { "topic1" => (0..100).to_a },
      members: { "member1" => Metadata.new(["topic1"]) },
    },
    {
      name: "lots of members",
      topics: { "topic1" => (0..10).to_a, "topic2" => (0..10).to_a },
      members: Hash[(0..50).map { |i| ["member#{i}", Metadata.new(["topic1", "topic2"])] }]
    },
    {
      name: "odd number of partitions",
      topics: { "topic1" => (0..14).to_a },
      members: {
        "member1" => Metadata.new(["topic1"]),
        "member2" => Metadata.new(["topic1"])
      },
    },
    {
      name: "five topics, 10 partitions, 3 consumers",
      topics: { "topic1" => [0, 1], "topic2" => [0, 1], "topic3" => [0, 1], "topic4" => [0, 1], "topic5" => [0, 1] },
      members: {
        "member1" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"]),
        "member2" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"]),
        "member3" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"])
      },
    }
  ].each do |options|
    name, topics, members = options[:name], options[:topics], options[:members]
    it name do
      partitions = topics.flat_map {|topic, partition_ids|
        partition_ids.map {|i|
          double(:"partition#{i}", topic: topic, partition_id: i)
        }
      }

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect_all_partitions_assigned(topics, assignments)
      expect_even_assignments(topics, assignments)
    end
  end

  def expect_all_partitions_assigned(topics, assignments)
    topics.each do |topic, partition_ids|
      partition_ids.each do |partition_id|
        assigned = assignments.values.find do |assigned_partitions|
          assigned_partitions.find {|assigned_partition|
            assigned_partition.topic == topic && assigned_partition.partition_id == partition_id
          }
        end
        expect(assigned).to_not be_nil
      end
    end
  end

  def expect_even_assignments(topics, assignments)
    num_partitions = topics.values.flatten.count
    assignments.values.each do |assigned_partition|
      num_assigned = assigned_partition.count
      expect(num_assigned).to be_within(1).of(num_partitions.to_f / assignments.count)
    end
  end

  context 'one consumer no subscriptions or topics / partitions' do
    it 'returns empty assignments' do
      members = { 'member1' => nil }
      partitions = []

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({})
    end
  end

  context 'one consumer with subscription but no matching topic partition' do
    it 'returns empty assignments' do
      members = { 'member1' => double(topics: ['topic1']) }
      partitions = []

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({})
    end
  end

  context 'one consumer subscribed to one topic with one partition' do
    it 'assigns the partition to the consumer' do
      members = { 'member1' => double(topics: ['topic1']) }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0]
      })
    end
  end

  context 'one consumer subscribed to one topic with multiple partitions' do
    it 'assigns all partitions to the consumer' do
      members = { 'member1' => double(topics: ['topic1']) }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0, t1p1]
      })
    end
  end

  context 'one consumer subscribed to one topic but with multiple different topic partitions' do
    it 'only assigns partitions for the subscribed topic' do
      members = { 'member1' => double(topics: ['topic1']) }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
        t2p0 = double(:"t2p0", topic: "topic2", partition_id: 0),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0, t1p1]
      })
    end
  end

  context 'one consumer subscribed to multiple topics' do
    it 'assigns all the topics partitions to the consumer' do
      members = { 'member1' => double(topics: ['topic1', 'topic2']) }

      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
        t2p0 = double(:"t2p0", topic: "topic2", partition_id: 0),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0, t1p1, t2p0]
      })
    end
  end

  context 'two consumers with one topic and only one partition' do
    it 'only assigns the partition to one consumer' do
      members = {
        'member1' => double(topics: ['topic1']),
        'member2' => double(topics: ['topic1'])
      }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0]
      })
    end
  end

  context 'two consumers subscribed to one topic with two partitions' do
    it 'assigns a partition to each consumer' do
      members = {
        'member1' => double(topics: ['topic1']),
        'member2' => double(topics: ['topic1'])
      }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0],
        'member2' => [t1p1]
      })
    end
  end

  context 'multiple consumers with mixed topics subscriptions' do
    it 'creates a balanced assignment' do
      members = {
        'member1' => double(topics: ['topic1']),
        'member2' => double(topics: ['topic1', 'topic2']),
        'member3' => double(topics: ['topic1'])
      }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
        t1p2 = double(:"t1p2", topic: "topic1", partition_id: 2),
        t2p0 = double(:"t2p0", topic: "topic2", partition_id: 0),
        t2p1 = double(:"t2p1", topic: "topic2", partition_id: 1),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0],
        'member2' => [t1p1, t2p0, t2p1],
        'member3' => [t1p2]
      })
    end
  end

  context 'two consumers subscribed to two topics with three partitions each' do
    it 'creates a balanced assignment' do
      members = {
        'member1' => double(topics: ['topic1', 'topic2']),
        'member2' => double(topics: ['topic1', 'topic2'])
      }
      partitions = [
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
        t1p2 = double(:"t1p2", topic: "topic1", partition_id: 2),
        t2p0 = double(:"t2p0", topic: "topic2", partition_id: 0),
        t2p1 = double(:"t2p1", topic: "topic2", partition_id: 1),
        t2p2 = double(:"t2p2", topic: "topic2", partition_id: 2),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments).to eq({
        'member1' => [t1p0, t1p2, t2p1],
        'member2' => [t1p1, t2p0, t2p2]
      })
    end
  end

  context 'many consumers subscribed to one topic with partitions given out of order' do
    it 'produces balanced assignments' do
      members = {
        'member1' => double(topics: ['topic1']),
        'member2' => double(topics: ['topic1']),
        'member3' => double(topics: ['topic2']),
      }

      partitions = [
        t2p0 = double(:"t2p0", topic: "topic2", partition_id: 0),
        t1p0 = double(:"t1p0", topic: "topic1", partition_id: 0),
        t2p1 = double(:"t2p1", topic: "topic2", partition_id: 1),
        t1p1 = double(:"t1p1", topic: "topic1", partition_id: 1),
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      # Without sorting the partitions by topic this input would produce a non balanced assignment:
      # member1 => [t1p0, t1p1]
      # member2 => []
      # member3 => [t2p0, t2p1]
      expect(assignments).to eq({
        'member1' => [t1p0],
        'member2' => [t1p1],
        'member3' => [t2p0, t2p1]
      })
    end
  end
end
