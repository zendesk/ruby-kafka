# frozen_string_literal: true

class FakeTokenProvider
    def initialize
    end

    def token
        "SASLOauthbearerMessageTest"
    end

    def extensions
        { test_key: "test_value", test_key_2: "test_value_2" }
    end
end