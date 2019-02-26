# frozen_string_literal: true

class FakeTokenProvider
  def token
    "SASLOAUTHBEARER.TEST_ID_TOKEN"
  end

  def extensions
    { test_key: "test_value", test_key_2: "test_value_2" }
  end
end

class FakeTokenProviderNoExtensions
  def token
    "SASLOAUTHBEARER.TEST_ID_TOKEN"
  end
end

class FakeBrokenTokenProvider
end
