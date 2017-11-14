require "kafka/datadog"

describe Kafka::Datadog::StatsdSubscriber do
  let(:subscriber_class) do
    Class.new(described_class) do
      def test_event
        event("something_happened", "yolo")
      end
    end
  end

  let(:subscriber) { subscriber_class.new }

  describe "#event" do
    it "publishes an event to Datadog" do
      subscriber.test_event
    end
  end
end
