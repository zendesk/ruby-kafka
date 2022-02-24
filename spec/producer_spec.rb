# frozen_string_literal: true

require "fake_broker"
require "fake_producer_interceptor"
require "timecop"
require "kafka/partitioner"
require "kafka/interceptors"

describe Kafka::Producer do
  let(:logger) { LOGGER }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test") }
  let(:broker1) { FakeBroker.new }
  let(:broker2) { FakeBroker.new }
  let(:compressor) { double(:compressor) }
  let(:cluster) { double(:cluster) }
  let(:transaction_manager) { double(:transaction_manager) }
  let(:producer) { initialize_producer }
  let(:partitioner) { Kafka::Partitioner.new }

  before do
    allow(cluster).to receive(:mark_as_stale!)
    allow(cluster).to receive(:refresh_metadata_if_necessary!)
    allow(cluster).to receive(:add_target_topics)

    allow(cluster).to receive(:get_leader).with("greetings", 0) { broker1 }
    allow(cluster).to receive(:get_leader).with("greetings", 1) { broker2 }
    allow(cluster).to receive(:disconnect).and_return(nil)

    allow(compressor).to receive(:compress) {|message_set| message_set }

    allow(transaction_manager).to receive(:idempotent?).and_return(false)
    allow(transaction_manager).to receive(:transactional?).and_return(false)
    allow(transaction_manager).to receive(:next_sequence_for).and_return(0)
    allow(transaction_manager).to receive(:producer_id).and_return(-1)
    allow(transaction_manager).to receive(:producer_epoch).and_return(0)
    allow(transaction_manager).to receive(:transactional_id).and_return(nil)
    allow(transaction_manager).to receive(:send_offsets_to_txn).and_return(nil)
    allow(transaction_manager).to receive(:close).and_return(nil)
  end

  describe "#extract_undelivered_messages!" do
    it "gets messages from the buffer" do
      producer.produce('test', topic: 'test')
      allow(cluster).to receive(:partitions_for).with('test') { [1] }
      producer.send(:assign_partitions!)
      messages = producer.extract_undelivered_messages!
      expect(messages).to eq([['test', { topic: 'test' }]])
    end

    it "gets messages from the pending message queue" do
      producer.produce('test', topic: 'test')
      messages = producer.extract_undelivered_messages!
      expect(messages).to eq([['test', { topic: 'test' }]])
    end

    it "clears buffers afterwards" do
      producer.produce('test', topic: 'test')
      expect(producer.buffer_size).to eq(1)
      producer.extract_undelivered_messages!
      expect(producer.buffer_size).to eq(0)
    end
  end

  describe "#produce" do
    before do
      allow(cluster).to receive(:partitions_for).with("greetings") { [0, 1, 2, 3] }
    end

    it "writes the message to the buffer" do
      producer.produce("hello", key: "greeting1", topic: "greetings")

      expect(producer.buffer_size).to eq 1
    end

    it "allows explicitly setting the partition" do
      producer.produce("hello", key: "greeting1", topic: "greetings", partition: 1)

      expect(producer.buffer_size).to eq 1
    end

    it "allows implicitly setting the partition using a partition key" do
      producer.produce("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(producer.buffer_size).to eq 1
    end

    it "raises BufferOverflow if the max buffer size is exceeded" do
      producer = initialize_producer(max_buffer_size: 2)

      producer.produce("hello1", topic: "greetings", partition: 0)
      producer.produce("hello1", topic: "greetings", partition: 0)

      expect {
        producer.produce("hello1", topic: "greetings", partition: 0)
      }.to raise_error(Kafka::BufferOverflow)

      expect(producer.buffer_size).to eq 2
    end

    it "works even when Kafka is unavailable" do
      allow(broker1).to receive(:produce).and_raise(Kafka::Error)
      allow(broker2).to receive(:produce).and_raise(Kafka::Error)

      producer.produce("hello", key: "greeting1", topic: "greetings", partition_key: "hey")

      expect(producer.buffer_size).to eq 1

      # Only when we try to send the messages to Kafka do we experience the issue.
      expect {
        producer.deliver_messages
      }.to raise_exception(Kafka::Error)
    end

    it "requires `partition` to be an Integer" do
      expect {
        producer.produce("hello", topic: "greetings", partition: "x")
      }.to raise_exception(ArgumentError)
    end

    it "converts the value to a string" do
      producer.produce(42, topic: "greetings", partition: 0)

      producer.deliver_messages

      expect(broker1.messages.map(&:value)).to eq ["42"]
    end

    it "doesn't convert the value to a string if it's nil" do
      producer.produce(nil, topic: "greetings", partition: 0)

      producer.deliver_messages

      expect(broker1.messages.map(&:value)).to eq [nil]
    end

    it "converts `key` to a string" do
      producer.produce("hello", topic: "greetings", key: 42, partition: 0)

      producer.deliver_messages

      expect(broker1.messages.map(&:key)).to eq ["42"]
    end

    it "doesn't convert `key` to a string if it's nil" do
      producer.produce("hello", topic: "greetings", key: nil, partition: 0)

      producer.deliver_messages

      expect(broker1.messages.map(&:key)).to eq [nil]
    end

    it "requires `topic` to be a String" do
      expect {
        producer.produce("hello", topic: :topic)
      }.to raise_exception(NoMethodError, /to_str/)
    end
  end

  describe "#deliver_messages" do
    let(:now) { Time.now }

    before(:each) do
      Timecop.freeze(now)
    end

    after(:each) do
      Timecop.return
    end

    it "sends messages to the leader of the partition being written to" do
      producer.produce("hello1", key: "greeting1", topic: "greetings", partition: 0)
      producer.produce("hello2", key: "greeting2", topic: "greetings", partition: 1)

      producer.deliver_messages

      expect(broker1.messages.map(&:value)).to eq ["hello1"]
      expect(broker2.messages.map(&:value)).to eq ["hello2"]
    end

    it "handles when a partition temporarily doesn't have a leader" do
      broker1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 5)

      producer.produce("hello1", topic: "greetings", partition: 0, headers: { hello: 'World' })

      expect { producer.deliver_messages }.to raise_error(Kafka::DeliveryFailed) {|exception|
        expect(exception.failed_messages).to eq [
          Kafka::PendingMessage.new(
            value: "hello1",
            key: nil,
            headers: {
              hello: 'World'
            },
            topic: "greetings",
            partition: 0,
            partition_key: nil,
            create_time: now
          )
        ]
      }

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      broker1.mark_partition_with_error(topic: "greetings", partition: 0, error_code: 0)

      producer.deliver_messages

      expect(producer.buffer_size).to eq 0
    end

    it "handles multiple messages for a partition during retry" do
      2.times do |i|
        producer.produce("hello#{i}", topic: "greetings", key: "key")
      end

      # Raise an error when requesting partitions for the first message then
      # return successfully for subsequent calls
      partitions_for_call_count = 0
      allow(cluster).to receive(:partitions_for) do
        partitions_for_call_count += 1
        raise(Kafka::UnknownTopicOrPartition.new) if partitions_for_call_count == 1
        [0, 1]
      end

      producer.deliver_messages
      expect(broker1.messages).to be_empty
      expect(broker2.messages.map(&:value)).to eq(%w(hello0 hello1))
    end

    it "handles when there's a connection error when fetching topic metadata" do
      allow(cluster).to receive(:get_leader).and_raise(Kafka::ConnectionError)

      producer.produce("hello1", topic: "greetings", partition: 0, headers: { hello: 'World' })

      expect { producer.deliver_messages }.to raise_error(Kafka::DeliveryFailed) {|exception|
        expect(exception.failed_messages).to eq([
          Kafka::PendingMessage.new(
            value: "hello1",
            key: nil,
            headers: {
              hello: 'World'
            },
            topic: "greetings",
            partition: 0,
            partition_key: nil,
            create_time: now
          )
        ])
      }

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      allow(cluster).to receive(:get_leader) { broker1 }

      producer.deliver_messages

      expect(producer.buffer_size).to eq 0
    end

    it "handles when there's a connection error when refreshing cluster metadata" do
      allow(cluster).to receive(:refresh_metadata_if_necessary!).and_raise(Kafka::ConnectionError)

      producer.produce("hello1", topic: "greetings", partition: 0, headers: { hello: 'World' })

      expect { producer.deliver_messages }.to raise_error(Kafka::DeliveryFailed) {|exception|
        expect(exception.failed_messages).to eq [
          Kafka::PendingMessage.new(
            value: "hello1",
            key: nil,
            headers: {
              hello: 'World'
            },
            topic: "greetings",
            partition: 0,
            partition_key: nil,
            create_time: now
          )
        ]
      }

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 1

      # Clear the error.
      allow(cluster).to receive(:refresh_metadata_if_necessary!)

      producer.deliver_messages

      expect(producer.buffer_size).to eq 0
    end

    it "clears the buffer after sending messages if no acknowledgements are required" do
      producer = initialize_producer(
        required_acks: 0, # <-- this is the important bit.
        max_retries: 2,
      )

      producer.produce("hello1", topic: "greetings", partition: 0)
      producer.deliver_messages

      # The producer was not able to write the message, but it's still buffered.
      expect(producer.buffer_size).to eq 0
    end

    it "sends a notification when there's an error finding the leader for a partition" do
      allow(cluster).to receive(:get_leader).and_raise(Kafka::UnknownTopicOrPartition.new("hello"))

      events = []

      subscriber = proc {|*args|
        events << ActiveSupport::Notifications::Event.new(*args)
      }

      ActiveSupport::Notifications.subscribed(subscriber, "topic_error.producer.kafka") do
        producer.produce("hello1", topic: "greetings", partition: 0)
        expect { producer.deliver_messages }.to raise_error(Kafka::DeliveryFailed)
      end

      event = events.last

      expect(event.payload[:topic]).to eq "greetings"
      expect(event.payload[:exception]).to eq ["Kafka::UnknownTopicOrPartition", "hello"]
    end

    it "sends a notification when there's an error writing messages to a partition" do
      broker1.mark_partition_with_error(
        topic: "greetings",
        partition: 0,
        error_code: 3,
      )

      events = []

      subscriber = proc {|*args|
        events << ActiveSupport::Notifications::Event.new(*args)
      }

      ActiveSupport::Notifications.subscribed(subscriber, "topic_error.producer.kafka") do
        producer.produce("hello1", topic: "greetings", partition: 0)
        expect { producer.deliver_messages }.to raise_error(Kafka::DeliveryFailed)
      end

      event = events.last

      expect(event.payload[:topic]).to eq "greetings"
      expect(event.payload[:exception]).to eq ["Kafka::UnknownTopicOrPartition", "Kafka::UnknownTopicOrPartition"]
    end
  end

  describe "#clear_buffer" do
    it "clears all pending messages" do
      producer.produce("hello1", topic: "greetings", partition: 0)
      expect(producer.buffer_size).to eq 1
      producer.clear_buffer
      expect(producer.buffer_size).to eq 0
    end
  end

  describe "#send_offsets_to_transaction" do
    let(:topic) { 'some_topic' }
    let(:partition) { rand(2**31) }
    let(:last_offset) { rand(2**31) }
    let(:leader_epoch) { Time.now.to_i }
    let(:group_id) { SecureRandom.uuid }
    let(:batch) do
      double(
        topic: topic,
        partition: partition,
        last_offset: last_offset,
        leader_epoch: leader_epoch
      )
    end
    it 'sends offsets to transaction manager' do
      producer.send_offsets_to_transaction(batch: batch, group_id: group_id)
      expect(transaction_manager).to have_received(:send_offsets_to_txn).with(
        offsets: {
          topic => {
            partition => {
              leader_epoch: leader_epoch,
              offset: last_offset + 1
            }
          }
        },
        group_id: group_id
      )
    end
  end

  describe '#interceptor' do
    let(:now) { Time.now }
    let(:pending_message) {
      Kafka::PendingMessage.new(
        value: "hello1",
        key: nil,
        headers: {
          hello: 'World'
        },
        topic: "greetings",
        partition: 0,
        partition_key: nil,
        create_time: now
      )
    }

    it "creates and shuts down a producer with interceptor" do
      interceptor = FakeProducerInterceptor.new
      producer = initialize_producer(
        interceptors: [interceptor],
        cluster: cluster,
        transaction_manager: transaction_manager
      )

      producer.shutdown
    end

    it "chains call" do
      interceptor1 = FakeProducerInterceptor.new(append_s: 'hello2')
      interceptor2 = FakeProducerInterceptor.new(append_s: 'hello3')
      interceptors = Kafka::Interceptors.new(interceptors: [interceptor1, interceptor2], logger: logger)
      intercepted_message = interceptors.call(pending_message)

      expect(intercepted_message).to eq Kafka::PendingMessage.new(
        value: "hello1hello2hello3",
        key: nil,
        headers: {
          hello: 'World'
        },
        topic: "greetings",
        partition: 0,
        partition_key: nil,
        create_time: now
      )
    end

    it "does not break the call chain" do
      interceptor1 = FakeProducerInterceptor.new(append_s: 'hello2', on_call_error: true)
      interceptor2 = FakeProducerInterceptor.new(append_s: 'hello3')
      interceptors = Kafka::Interceptors.new(interceptors: [interceptor1, interceptor2], logger: logger)
      intercepted_message = interceptors.call(pending_message)

      expect(intercepted_message).to eq Kafka::PendingMessage.new(
        value: "hello1hello3",
        key: nil,
        headers: {
          hello: 'World'
        },
        topic: "greetings",
        partition: 0,
        partition_key: nil,
        create_time: now
      )
    end

    it "returns original message when all interceptors fail" do
      interceptor1 = FakeProducerInterceptor.new(append_s: 'hello2', on_call_error: true)
      interceptor2 = FakeProducerInterceptor.new(append_s: 'hello3', on_call_error: true)
      interceptors = Kafka::Interceptors.new(interceptors: [interceptor1, interceptor2], logger: logger)
      intercepted_message = interceptors.call(pending_message)

      expect(intercepted_message).to eq pending_message
    end
  end

  def initialize_producer(**options)
    default_options = {
      cluster: cluster,
      transaction_manager: transaction_manager,
      logger: logger,
      instrumenter: instrumenter,
      max_retries: 2,
      retry_backoff: 0,
      compressor: compressor,
      ack_timeout: 10,
      required_acks: 1,
      max_buffer_size: 1000,
      max_buffer_bytesize: 10_000_000,
      partitioner: partitioner,
    }

    Kafka::Producer.new(**default_options.merge(options))
  end
end
