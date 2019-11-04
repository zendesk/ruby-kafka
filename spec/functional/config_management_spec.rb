# frozen_string_literal: true

describe "Config management API", functional: true do
  let(:broker_id) { kafka.controller_broker.node_id }

  example "describe config" do
    unless kafka.supports_api?(Kafka::Protocol::DESCRIBE_CONFIGS_API)
      skip("This Kafka version not support DescribeConfigs for broker configs")
    end

    expect(kafka.describe_configs(broker_id, ['background.threads']).first.value.to_i).to eq(10)
  end
end
