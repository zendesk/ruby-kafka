# frozen_string_literal: true

describe "API Versions API", functional: true do
  example "getting the API versions that are supported by the Kafka brokers" do
    produce_api = kafka.apis.find {|v| v.api_key == 0 }

    expect(produce_api.min_version).to eq 0
    expect(produce_api.max_version).to be >= 2
  end

  example "checks cluster API support" do
    expect(kafka.supports_api?(Kafka::Protocol::PRODUCE_API)).to eql(true)
    expect(kafka.supports_api?(Kafka::Protocol::PRODUCE_API, 0)).to eql(true)
    expect(kafka.supports_api?(Kafka::Protocol::PRODUCE_API, 100)).to eql(false)
    expect(kafka.supports_api?(100)).to eql(false)
    expect(kafka.supports_api?(100, 100)).to eql(false)
  end
end
