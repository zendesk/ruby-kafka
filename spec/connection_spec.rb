require "kafka/connection"
require "logger"
require "stringio"

describe Kafka::Connection do
  describe "open" do
    let(:log) { LOG }
    let(:logger) { Logger.new(log) }

    it "connects to a Kafka broker" do
      connection = Kafka::Connection.open(
        host: KAFKA_HOST,
        port: KAFKA_PORT,
        client_id: "test",
        logger: logger,
      )
    end

    it "raises ConnectionError if it cannot connect to the broker" do
      expect {
        Kafka::Connection.open(
          host: "imaginary.example",
          port: 1234,
          client_id: "test",
          logger: logger,
        )
      }.to raise_error(Kafka::ConnectionError)
    end
  end
end
