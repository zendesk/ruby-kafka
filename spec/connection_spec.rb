require "kafka/connection"
require "logger"
require "stringio"

describe Kafka::Connection do
  describe "open" do
    let(:log) { StringIO.new }
    let(:logger) { Logger.new(log) }

    it "connects to a Kafka broker" do
      connection = Kafka::Connection.new(
        host: ENV.fetch("KAFKA_HOST"),
        port: ENV.fetch("KAFKA_PORT"),
        client_id: "test",
        logger: logger,
      )

      connection.open
    end

    it "raises ConnectionError if it cannot connect to the broker" do
      connection = Kafka::Connection.new(
        host: "imaginary.example",
        port: 1234,
        client_id: "test",
        logger: logger,
      )

      expect { connection.open }.to raise_error(Kafka::ConnectionError)
    end
  end
end
