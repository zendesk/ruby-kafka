require "docker"

if ENV.key?("DOCKER_HOST")
  Docker.url = ENV.fetch("DOCKER_HOST")
end

class TestCluster
  DOCKER_HOSTNAME = URI(ENV.fetch("DOCKER_HOST", "localhost")).host
  KAFKA_IMAGE = "ches/kafka:0.9.0.1"
  ZOOKEEPER_IMAGE = "jplock/zookeeper:3.4.6"
  KAFKA_CLUSTER_SIZE = 3

  def initialize
    [KAFKA_IMAGE, ZOOKEEPER_IMAGE].each do |image|
      print "Fetching image #{image}... "

      unless Docker::Image.exist?(image)
        Docker::Image.create("fromImage" => image)
      end

      puts "OK"
    end

    @zookeeper = create(
      "Image" => ZOOKEEPER_IMAGE,
      "ExposedPorts" => {
        "2181/tcp" => {}
      },
    )

    @kafka_brokers = KAFKA_CLUSTER_SIZE.times.map {|broker_id|
      port = 9092 + broker_id

      create(
        "Image" => KAFKA_IMAGE,
        "Hostname" => "localhost",
        "Links" => ["#{@zookeeper.id}:zookeeper"],
        "ExposedPorts" => {
          "9092/tcp" => {}
        },
        "Env" => [
          "KAFKA_BROKER_ID=#{broker_id}",
          "KAFKA_ADVERTISED_HOST_NAME=#{DOCKER_HOSTNAME}",
          "KAFKA_ADVERTISED_PORT=#{port}",
        ]
      )
    }

    @kafka = @kafka_brokers.first
  end

  def start
    @zookeeper.start(
      "PortBindings" => {
        "2181/tcp" => [{ "HostPort" => "" }]
      }
    )

    @kafka_brokers.each_with_index do |kafka, index|
      port = 9092 + index

      kafka.start(
        "PortBindings" => {
          "9092/tcp" => [{ "HostPort" => "#{port}/tcp" }]
        },
      )
      Thread.new do
        kafka.attach { |stream, chunk| puts "#{index}: #{chunk}" }
      end
    end

    ensure_kafka_is_ready
  end

  def kafka_hosts
    @kafka_brokers.map {|kafka|
      config = kafka.json.fetch("NetworkSettings").fetch("Ports")
      port = config.fetch("9092/tcp").first.fetch("HostPort")
      host = DOCKER_HOSTNAME

      "#{host}:#{port}"
    }
  end

  def kill_kafka_broker(number)
    broker = @kafka_brokers[number]
    puts "Killing broker #{number}"
    broker.kill
  end

  def start_kafka_broker(number)
    broker = @kafka_brokers[number]
    puts "Starting broker #{number}"
    broker.start
  end

  def create_topic(topic, num_partitions: 1, num_replicas: 1)
    command = [
      "/kafka/bin/kafka-topics.sh",
      "--create",
      "--topic=#{topic}",
      "--replication-factor=#{num_replicas}",
      "--partitions=#{num_partitions}",
      "--zookeeper=zookeeper",
    ]

    container = create(
      "Image" => KAFKA_IMAGE,
      "Links" => ["#{@zookeeper.id}:zookeeper"],
      "Cmd" => command,
    )

    print "Creating topic #{topic}... "

    begin
      container.start
      status = container.wait.fetch('StatusCode')

      if status != 0
        puts "failed!"
        puts container.logs(stdout: true, stderr: true)
        raise "Command failed with status #{status}"
      end
    ensure
      container.delete(force: true)
    end

    puts "OK"
  end

  def stop
    puts "Stopping cluster..."

    @kafka_brokers.each {|kafka| kafka.delete(force: true) rescue nil }
    @zookeeper.delete(force: true) rescue nil
  end

  private

  def ensure_kafka_is_ready
    kafka_hosts.each do |host_and_port|
      host, port = host_and_port.split(":", 2)

      loop do
        begin
          print "Waiting for #{host_and_port}... "
          socket = TCPSocket.open(host, port)
          socket.close
          puts "OK"
          break
        rescue
          puts "not ready"
          sleep 1
        end
      end
    end
  end

  def create(options)
    Docker::Container.create(options)
  end
end

KAFKA_CLUSTER = TestCluster.new
KAFKA_CLUSTER.start

KAFKA_BROKERS = KAFKA_CLUSTER.kafka_hosts

host, port = KAFKA_BROKERS.first.split(":", 2)

KAFKA_HOST = host
KAFKA_PORT = port.to_i

at_exit {
  KAFKA_CLUSTER.stop
}
