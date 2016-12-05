describe Kafka::Connection do
  let(:logger) { LOGGER }
  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }

  let(:connection) {
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test",
      logger: logger,
      instrumenter: Kafka::Instrumenter.new(client_id: "test"),
      connect_timeout: 0.1,
      socket_timeout: 0.1,
    )
  }

  let!(:broker) { FakeServer.start(server) }

  class FakeServer
    def self.start(server)
      thread = Thread.new { new(server).start }
      thread.abort_on_exception = true
      thread
    end

    def initialize(server)
      @server = server
    end

    def start
      loop do
        client = @server.accept

        begin
          handle_client(client)
        rescue => e
          puts e
          break
        ensure
          client.close
        end
      end
    end

    def handle_client(client)
      loop do
        request_bytes = Kafka::Protocol::Decoder.new(client).bytes
        request_decoder = Kafka::Protocol::Decoder.new(StringIO.new(request_bytes))

        api_key = request_decoder.int16
        api_version = request_decoder.int16
        correlation_id = request_decoder.int32
        client_id = request_decoder.string

        message = request_decoder.string

        response = StringIO.new
        response_encoder = Kafka::Protocol::Encoder.new(response)
        response_encoder.write_int32(correlation_id)
        response_encoder.write_string(message)

        encoder = Kafka::Protocol::Encoder.new(client)
        encoder.write_bytes(response.string)
      end
    end
  end

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

    it "skips responses to previous requests if there's no async response in the pipeline" do
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

  describe "#send_async_request" do
    let(:api_key) { 0 }
    let(:response_decoder) { double(:response_decoder) }

    before do
      allow(response_decoder).to receive(:decode) {|decoder|
        decoder.string
      }
    end

    it "asynchronously sends the request" do
      request1 = double(:request1, api_key: api_key, response_class: response_decoder)
      request2 = double(:request2, api_key: api_key, response_class: response_decoder)

      allow(request1).to receive(:encode) {|encoder| encoder.write_string("response1") }
      allow(request2).to receive(:encode) {|encoder| encoder.write_string("response2") }

      response1 = connection.send_async_request(request1)
      response2 = connection.send_async_request(request2)

      expect(response1.call).to eq "response1"
      expect(response2.call).to eq "response2"
    end

    it "allows blocking on later requests before previous ones" do
      request1 = double(:request1, api_key: api_key, response_class: response_decoder)
      request2 = double(:request2, api_key: api_key, response_class: response_decoder)

      allow(request1).to receive(:encode) {|encoder| encoder.write_string("response1") }
      allow(request2).to receive(:encode) {|encoder| encoder.write_string("response2") }

      response1 = connection.send_async_request(request1)
      response2 = connection.send_async_request(request2)

      expect(response2.call).to eq "response2"
      expect(response1.call).to eq "response1"
    end
  end
end
