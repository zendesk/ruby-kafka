describe Kafka::GroupProtocolManager do
  let(:cluster) { double(:cluster) }
  let(:strategy_cpu) { Class.new }
  let(:strategy_ram) { Class.new }

  describe '#group_protocols' do
    context 'empty input protocols' do
      let(:manager) { Kafka::GroupProtocolManager.new(cluster: cluster) }

      it 'pushes standard protocol with round robin strategy' do
        expect(manager.group_protocols.length).to eql(1)
        protocol = manager.group_protocols.first
        expect(protocol.name).to eql('standard')
        expect(protocol.metadata).to eql('')
        expect(protocol.assignment_strategy).to be_a(Kafka::RoundRobinAssignmentStrategy)
      end
    end

    context 'input group protocols is available' do
      let(:manager) {
        Kafka::GroupProtocolManager.new(
          cluster: cluster,
          group_protocols: [
            {
              name: 'cpu',
              metadata: '5',
              assignment_strategy: strategy_cpu
            },
            {
              name: 'ram',
              metadata: '3G',
              assignment_strategy: strategy_ram
            }
          ]
        )
      }

      it "returns list of protocol instances without default standard protocol" do
        expect(manager.group_protocols.length).to eql(2)

        protocol = manager.group_protocols[0]
        expect(protocol.name).to eql('cpu')
        expect(protocol.metadata).to eql('5')
        expect(protocol.assignment_strategy).to be_a(strategy_cpu)

        protocol = manager.group_protocols[1]
        expect(protocol.name).to eql('ram')
        expect(protocol.metadata).to eql('3G')
        expect(protocol.assignment_strategy).to be_a(strategy_ram)
      end
    end
  end

  describe '#assign' do
    before do
      partitions = (0..30).map {|i| double(:"partition#{i}", partition_id: i) }
      allow(cluster).to receive(:partitions_for) { partitions }
    end

    let(:manager) {
      Kafka::GroupProtocolManager.new(
        cluster: cluster,
        group_protocols: [
          {
            name: 'cpu',
            metadata: '5',
            assignment_strategy: strategy_cpu
          },
          {
            name: 'ram',
            metadata: '3G',
            assignment_strategy: strategy_ram
          }
        ]
      )
    }

    context 'group protocol is available' do
      it 'calls corresponding strategy to assign partitions' do
        group_assignment = double(:group_assignment)

        expect_any_instance_of(strategy_ram).to receive(:assign).with(
          members: { 'member_1' => '3G', 'member_2' => '10G' },
          topics: {
            'greetings' => (0..30).to_a
          }
        ).and_return(group_assignment)

        result = manager.assign(
          group_protocol: 'ram',
          topics: ['greetings'],
          members: { 'member_1' => '3G', 'member_2' => '10G' }
        )
        expect(result).to eql(group_assignment)
      end
    end

    context 'group protocol is not available' do
      it 'uses standard protocol with round robin strategy' do
        expect_any_instance_of(Kafka::RoundRobinAssignmentStrategy).to receive(:assign).with(
          members: { 'member_1' => '3G', 'member_2' => '10G' },
          topics: {
            'greetings' => (0..30).to_a
          }
        )
        manager.assign(
          group_protocol: 'non_exist',
          topics: ['greetings'],
          members: { 'member_1' => '3G', 'member_2' => '10G' }
        )
      end
    end
  end
end
