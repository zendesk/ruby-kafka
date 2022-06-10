require 'kafka'

broker_address = ["msk-cluster-region.amazonaws.com:9098"]
role_arn = "arn:aws:iam::AWS_account:role/********"
role_session_name = "example_session"
test_topic = "test_topic"
region = "us-east-2"


# No explicit credentials needed, just other config values.
sts = Aws::STS::Client.new

# to get more information about how to use AssumeRoleCredentials
# please refer to https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/AssumeRoleCredentials.html for more details
role_credentials = Aws::AssumeRoleCredentials.new(
  client: sts,
  role_arn: role_arn,
  role_session_name: role_session_name
)

kafka_client = Kafka.new(
  broker_address,
  aws_iam_assume_role_credentials: role_credentials,
  sasl_aws_msk_iam_aws_region: region,
  ssl_ca_certs_from_system: true,
)


producer = kafka_client.producer
producer.produce("hello1", topic: test_topic)
producer.deliver_messages
