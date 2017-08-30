require 'fake_server'

describe Kafka::Connection do
  let(:logger) { LOGGER }
  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }

  let(:sasl_authenticator) {
    instance_double(Kafka::SaslAuthenticator, authenticate!: true)
  }
  let(:connection) {
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test",
      logger: logger,
      instrumenter: Kafka::Instrumenter.new(client_id: "test"),
      connect_timeout: 0.1,
      socket_timeout: 0.1,
      sasl_authenticator: sasl_authenticator
    )
  }

  let!(:broker) { FakeServer.start(server) }

  describe "#send_request" do
    let(:api_key) { 0 }
    let(:request) { double(:request, api_key: api_key) }
    let(:response_decoder) { double(:response_decoder) }

    before do
      allow(request).to receive(:encode) {|encoder| encoder.write_string("hello!") }
      allow(request).to receive(:response_class) { response_decoder }

      allow(response_decoder).to receive(:decode) {|decoder|
        decoder.string
      }
    end

    it "sends requests to a broker and reads back the response" do
      response = connection.send_request(request)

      expect(response).to eq "hello!"
    end

    it "skips responses to previous requests" do
      # By passing nil as the final argument we're telling Connection that we're
      # not expecting a response, so it won't read one. However, the fake broker
      # *is* writing a response, so we'll get that the next time we read a response,
      # causing a mismatch. This simulates the client killing the connection due
      # to e.g. a timeout, then resuming with a new request -- the old response
      # still sits in the connection waiting to be read.
      allow(request).to receive(:response_class) { nil }

      connection.send_request(request)

      allow(request).to receive(:encode) {|encoder| encoder.write_string("goodbye!") }
      allow(request).to receive(:response_class) { response_decoder }
      response = connection.send_request(request)

      expect(response).to eq "goodbye!"
    end

    it "disconnects on network errors" do
      response = connection.send_request(request)

      expect(response).to eq "hello!"

      broker.kill

      expect {
        connection.send_request(request)
      }.to raise_error(Kafka::ConnectionError)
    end

    it "calls authenticate when a new connection is open" do
      expect(sasl_authenticator).to receive(:authenticate!).with(connection).once

      response = connection.send_request(request)
      connection.send_request(request)
      connection.send_request(request)

      expect(response).to eq "hello!"
    end

    it "re-opens the connection after a network error" do
      connection.send_request(request)
      broker.kill

      # Connection is torn down
      connection.send_request(request) rescue nil

      server.close
      server = TCPServer.new(host, port)
      broker = FakeServer.start(server)

      # Connection is re-established
      response = connection.send_request(request)

      expect(response).to eq "hello!"
    end

    it "emits a notification" do
      events = []

      subscriber = proc {|*args|
        events << ActiveSupport::Notifications::Event.new(*args)
      }

      ActiveSupport::Notifications.subscribed(subscriber, "request.connection.kafka") do
        connection.send_request(request)
      end

      expect(events.count).to eq 1

      event = events.first

      expect(event.payload[:api]).to eq :produce
      expect(event.payload[:request_size]).to eq 22
      expect(event.payload[:response_size]).to eq 12
    end
  end
end
