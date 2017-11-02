describe "API Versions API", functional: true do
  example "getting the API versions that are supported by the Kafka brokers" do
    produce_api = kafka.apis.find {|v| v.api_key == 0 }

    expect(produce_api.min_version).to eq 0
    expect(produce_api.max_version).to eq 2
  end
end
