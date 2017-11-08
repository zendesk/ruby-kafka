require 'fake_server'
require 'kafka/sasl_scram_authenticator'

describe Kafka::SaslScramAuthenticator do
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

  describe '#authenticate!' do
    context 'when correct username/password using sha256' do
      let(:sasl_scram_authenticator) {
        Kafka::SaslScramAuthenticator.new(
          'spec_username',
          'spec_password',
          logger: logger,
          mechanism: 'sha256',
          connection: connection
        )
      }

      it 'successfully authenticates' do
        expect(sasl_scram_authenticator.authenticate!).to be_truthy
      end
    end
    context 'when correct username/password using sha512' do
      let(:sasl_scram_authenticator) {
        Kafka::SaslScramAuthenticator.new(
          'spec_username',
          'spec_password',
          logger: logger,
          mechanism: 'sha512',
          connection: connection
        )
      }

      it 'successfully authenticates' do
        expect(sasl_scram_authenticator.authenticate!).to be_truthy
      end
    end
    context 'when incorrect username' do
      let(:sasl_scram_authenticator) {
        Kafka::SaslScramAuthenticator.new(
          'spec_wrong_username',
          'spec_password',
          logger: logger,
          mechanism: 'sha256',
          connection: connection
        )
      }

      it 'raise error' do
        expect { sasl_scram_authenticator.authenticate! }.to raise_error(Kafka::FailedScramAuthentication)
      end
    end
    context 'when incorrect password' do
      let(:sasl_scram_authenticator) {
        Kafka::SaslScramAuthenticator.new(
          'spec_username',
          'spec_wrong_password',
          logger: logger,
          mechanism: 'sha256',
          connection: connection
        )
      }

      it 'raise error' do
        expect { sasl_scram_authenticator.authenticate! }.to raise_error(Kafka::FailedScramAuthentication)
      end
    end
  end
end
