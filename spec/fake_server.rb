class FakeServer
  SUPPORTED_MECHANISMS = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']

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
    encoder = Kafka::Protocol::Encoder.new(client)
    decoder = Kafka::Protocol::Decoder.new(client)

    loop do
      request_bytes = decoder.bytes
      request_data = Kafka::Protocol::Decoder.new(StringIO.new(request_bytes));
      api_key = request_data.int16
      _api_version = request_data.int16
      correlation_id = request_data.int32
      _client_id = request_data.string

      message = request_data.string

      response = StringIO.new
      response_encoder = Kafka::Protocol::Encoder.new(response)
      response_encoder.write_int32(correlation_id)

      case api_key
      when 17 then
        response_encoder.write_int16(0)
        response_encoder.write_array(SUPPORTED_MECHANISMS) { |m| response_encoder.write_string(m) }
        encoder.write_bytes(response.string)
        auth(message, encoder, decoder)
        break
      else
        response_encoder.write_string(message)
        encoder.write_bytes(response.string)
      end
    end
  end

  def auth(auth_mechanism, encoder, decoder)
    case auth_mechanism
    when 'PLAIN'
      message = decoder.bytes
      _authzid, username, password = message.split("\000")
      if username == 'spec_username' && password == 'spec_password'
        encoder.write_bytes('')
      end
    when 'SCRAM-SHA-256', 'SCRAM-SHA-512'
      scram_sasl_authenticate(auth_mechanism[6..-1], encoder, decoder)
    else
      puts "UNKNOWN AUTH MECHANISM"
    end
  end

  def scram_sasl_authenticate(algorithm, encoder, decoder)
    zk_username = 'spec_username'
    zk_data = {
      'SHA-512' => {
        salt: 'ODVhbzNqcGdneDR5ZzIzbmJpcnpodmdxcg==',
        stored_key: 'kfUpWelvXn406F1rKx3gE9Nz6qBBI+7v1Dg2n8QSNy9ZA1vU1jxYKOMRVV9188TDxhQe6Te0D8R2t0r5YFILnA==',
        server_key: 'CDkccMty/z9z7KUciVixhIuPLV53QtMHT2SbJUbvNqdaqGvtkTwDgMCLjWKqMKkUvnInYziJh/YfRKYNoLEnaQ==',
        iterations: 4096
      },
      'SHA-256' => {
        salt: 'MWVkNGdvam9qNG4yYmt1dG82ZGxrY3ppM3c=',
        stored_key: 'W28WpOjPl87SPMfFZsuyA5Yor0Z/q4+VZJlZqzDfgsI=',
        server_key: '17y/jubvVV8cWGxhaMN/8eOFTvnaYQ9f/JJmNszmOFI=',
        iterations: 4096
      }
    }
    @scram_mechanism = algorithm
    request_bytes = decoder.bytes
    _, _, userdata, nouncedata = request_bytes.split(',')
    _, username = userdata.split('=')
    _, client_nounce = nouncedata.split('=')

    return if username != zk_username

    client_first_message_bare = "#{userdata},#{nouncedata}"
    server_nounce = SecureRandom.urlsafe_base64(8)

    salt64 = zk_data[algorithm][:salt]
    iterations = zk_data[algorithm][:iterations]
    stored_key = Base64.strict_decode64(zk_data[algorithm][:stored_key])
    server_key = Base64.strict_decode64(zk_data[algorithm][:server_key])
    salt = Base64.strict_decode64(salt64)

    server_first_message = "r=#{client_nounce}#{server_nounce},s=#{salt64},i=#{iterations}"
    encoder.write_bytes(server_first_message)

    request_bytes = decoder.bytes
    c, r, proofdata = request_bytes.split(",")
    _, proof = proofdata.split("=", 2)

    client_last_message_without_proof = "#{c},#{r}"
    auth_message = [client_first_message_bare, server_first_message, client_last_message_without_proof].join(',')
    salted_password = hi('spec_password', salt, iterations)
    client_key = hmac(salted_password, 'Client Key')
    client_signature = hmac(stored_key, auth_message)
    server_proof = Base64.strict_encode64(xor(client_key, client_signature))

    return if server_proof != proof

    server_signature = Base64.strict_encode64(hmac(server_key, auth_message))
    encoder.write_bytes("v=#{server_signature}")
  end

  def digest
    @digest ||= case @scram_mechanism
                when 'SHA-256'
                  OpenSSL::Digest::SHA256.new.freeze
                when 'SHA-512'
                  OpenSSL::Digest::SHA512.new.freeze
                else
                  raise StandardError, "Unknown mechanism '#{@scram_mechanism}'"
                end
  end

  def xor(first, second)
    first.bytes.zip(second.bytes).map { |(a, b)| (a ^ b).chr }.join('')
  end

  def hi(str, salt, iterations)
    OpenSSL::PKCS5.pbkdf2_hmac(
      str,
      salt,
      iterations,
      digest.size,
      digest
    )
  end

  def hmac(data, key)
    OpenSSL::HMAC.digest(digest, data, key)
  end

  def h(str)
    digest.digest(str)
  end
end
