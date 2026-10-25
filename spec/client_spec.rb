# frozen_string_literal: true

describe Kafka::Client do
  let(:logger) { LOGGER }
  let(:kafka_brokers) { KAFKA_BROKERS }
  let(:client_opts) do
    {
      seed_brokers: KAFKA_BROKERS,
      client_id: "test",
      logger: logger
    }
  end

  describe ".new" do
    context "with reconnect backoff options" do
      it "accepts seconds through Kafka.new, including a zero maximum when disabled" do
        expect(Kafka.new(kafka_brokers, reconnect_backoff: 0.1)).to be_a(Kafka::Client)
        expect(Kafka.new(kafka_brokers, reconnect_backoff: 0, reconnect_backoff_max: 0)).to be_a(Kafka::Client)
      end

      [:reconnect_backoff, :reconnect_backoff_max].each do |option|
        [nil, false, "0.1", -1, Float::NAN, Float::INFINITY, 10**400, Complex(1, 1)].each do |value|
          it "rejects #{option}=#{value.inspect} before network activity" do
            expect(Kafka::SocketWithTimeout).not_to receive(:new)
            expect {
              described_class.new(**client_opts.merge(option => value))
            }.to raise_error(ArgumentError, /#{option}.*finite.*non-negative/)
          end
        end
      end

      it "rejects a maximum below the initial delay" do
        expect {
          described_class.new(**client_opts.merge(reconnect_backoff: 2, reconnect_backoff_max: 1))
        }.to raise_error(ArgumentError, /reconnect_backoff_max.*reconnect_backoff/)
      end
    end

    context "when SASL SCRAM has been configured without ssl" do
      before do
        client_opts.update({
          sasl_scram_username: "spec_username",
          sasl_scram_password: "spec_password",
          sasl_scram_mechanism: "sha256"
        })
      end
      context "when sasl_over_ssl is unspecified" do
        it "raises ArgumentError due to missing SSL config" do
          expect {
            described_class.new(**client_opts)
          }.to raise_error(ArgumentError, /SASL authentication requires that SSL is configured/)
        end
      end

      context "when sasl_over_ssl is true" do
        before { client_opts.update(sasl_over_ssl: true) }

        it "raises ArgumentError due to missing SSL config" do
          expect {
            described_class.new(**client_opts)
          }.to raise_error(ArgumentError, /SASL authentication requires that SSL is configured/)
        end
      end

      context "when sasl_over_ssl is false" do
        before { client_opts.update(sasl_over_ssl: false) }

        it "creates a new Kafka::Client object" do
          expect { described_class.new(**client_opts) }.to_not raise_exception
        end
      end
    end
  end

  describe "#deliver_message" do
    subject(:client) { described_class.new(**client_opts) }

    it "requires `topic` to be a String" do
      expect {
        client.deliver_message("hello", topic: :topic)
      }.to raise_exception(NoMethodError, /to_str/)
    end
  end
end
