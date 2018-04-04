require "uri"

module Kafka
  module BrokerUri
    DEFAULT_PORT = 9092
    URI_SCHEMES = ["kafka", "kafka+ssl", "plaintext", "ssl"]

    # Parses a Kafka broker URI string.
    #
    # Examples of valid strings:
    # * `kafka1.something`
    # * `kafka1.something:1234`
    # * `kafka://kafka1.something:1234`
    # * `kafka+ssl://kafka1.something:1234`
    # * `plaintext://kafka1.something:1234`
    #
    # @param str [String] a Kafka broker URI string.
    # @return [URI]
    def self.parse(str)
      str = "kafka://" + str unless str =~ /:\/\//

      uri = URI.parse(str)
      uri.port ||= DEFAULT_PORT

      case uri.scheme
      when 'plaintext'
        uri.scheme = 'kafka'
      when 'ssl'
        uri.scheme = 'kafka+ssl'
      end

      unless URI_SCHEMES.include?(uri.scheme)
        raise Kafka::Error, "invalid protocol `#{uri.scheme}` in `#{str}`"
      end

      uri
    end
  end
end
