require 'fake_server'

describe Kafka::SaslPlainAuthenticator do
  let(:logger) { LOGGER }
  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }
  let(:authenticator) {
    instance_double(Kafka::Authenticator, authenticate!: true)
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
      authenticator: authenticator
    )
  }

  let!(:broker) { FakeServer.start(server) }

  describe '#authenticate!' do
    context 'when correct username/password' do
      let(:sasl_plain_authenticator) {
        Kafka::SaslPlainAuthenticator.new(
          connection: connection,
          logger: logger,
          authzid: 'spec_authzid',
          username: 'spec_username',
          password: 'spec_password'
        )
      }

      it 'successfully authenticates' do
        expect(sasl_plain_authenticator.authenticate!).to be_truthy
      end
    end

    context 'when incorrect username/password' do
      let(:sasl_plain_authenticator) {
        Kafka::SaslPlainAuthenticator.new(
          connection: connection,
          logger: logger,
          authzid: '',
          username: 'bad_username',
          password: 'bad_password'
        )
      }

      it 'raises Kafka::Error with EOFError' do
        expect { sasl_plain_authenticator.authenticate! }.to raise_error(Kafka::Error, 'SASL PLAIN authentication failed: EOFError')
      end
    end
  end
end
