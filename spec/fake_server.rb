class FakeServer
  SUPPORTED_MECHANISMS = ['PLAIN']

  def self.start(server)
    thread = Thread.new { new(server).start }
    thread.abort_on_exception = true
    thread
  end

  def initialize(server)
    @server = server
    @authenticating = false
    @auth_mechanism = nil
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
      encoder = Kafka::Protocol::Encoder.new(client)

      # Special case for authentication
      if @authenticating
        case @auth_mechanism
        when 'PLAIN'
          _authzid, username, password = request_bytes.split("\000")

          if username == 'spec_username' && password == 'spec_password'
            # Successfully Authenticated, send back empty string
            encoder.write_bytes('')
            @authenticating = false
          end
        else
          # Unknown mechanism
        end

        break
      end

      request_decoder = Kafka::Protocol::Decoder.new(StringIO.new(request_bytes))
      api_key = request_decoder.int16
      api_version = request_decoder.int16
      correlation_id = request_decoder.int32
      client_id = request_decoder.string

      message = request_decoder.string

      response = StringIO.new
      response_encoder = Kafka::Protocol::Encoder.new(response)
      response_encoder.write_int32(correlation_id)

      if api_key == 17 # SASL Authentication
        response_encoder.write_int16(0) # no errors
        response_encoder.write_array(SUPPORTED_MECHANISMS) { |msg| response_encoder.write_string(msg) }
        @authenticating = true
        @auth_mechanism = message
      else
        response_encoder.write_string(message)
      end
      encoder.write_bytes(response.string)
    end
  end
end
