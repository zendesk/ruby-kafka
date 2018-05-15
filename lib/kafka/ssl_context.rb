# frozen_string_literal: true

require "openssl"

module Kafka
  module SslContext

    def self.build(ca_cert_file_path: nil, ca_cert: nil, client_cert: nil, client_cert_key: nil, ca_certs_from_system: nil)
      return nil unless ca_cert_file_path || ca_cert || client_cert || client_cert_key || ca_certs_from_system

      ssl_context = OpenSSL::SSL::SSLContext.new

      if client_cert && client_cert_key
        ssl_context.set_params(
          cert: OpenSSL::X509::Certificate.new(client_cert),
          key: OpenSSL::PKey.read(client_cert_key)
        )
      elsif client_cert && !client_cert_key
        raise ArgumentError, "Kafka client initialized with `ssl_client_cert` but no `ssl_client_cert_key`. Please provide both."
      elsif !client_cert && client_cert_key
        raise ArgumentError, "Kafka client initialized with `ssl_client_cert_key`, but no `ssl_client_cert`. Please provide both."
      end

      if ca_cert || ca_cert_file_path || ca_certs_from_system
        store = OpenSSL::X509::Store.new
        Array(ca_cert).each do |cert|
          store.add_cert(OpenSSL::X509::Certificate.new(cert))
        end
        if ca_cert_file_path
          store.add_file(ca_cert_file_path)
        end
        if ca_certs_from_system
          store.set_default_paths
        end
        ssl_context.cert_store = store
      end

      ssl_context
    end
  end
end
