require "docker"

Docker.url = ENV.fetch("DOCKER_HOST")

class TestCluster
  DOCKER_HOSTNAME = URI(ENV.fetch("DOCKER_HOST")).host
  KAFKA_IMAGE = "wurstmeister/kafka:0.8.2.0"
  ZOOKEEPER_IMAGE = "wurstmeister/zookeeper:3.4.6"
  KAFKA_CLUSTER_SIZE = 3

  def initialize
    [KAFKA_IMAGE, ZOOKEEPER_IMAGE].each do |image|
      print "Fetching image #{image}... "
      Docker::Image.create("fromImage" => image)
      puts "OK"
    end

    @zookeeper = create(
      "Image" => ZOOKEEPER_IMAGE,
      "ExposedPorts" => {
        "2181/tcp" => {}
      },
    )

    @kafka_brokers = KAFKA_CLUSTER_SIZE.times.map {
      create(
        "Image" => KAFKA_IMAGE,
        "Links" => ["#{@zookeeper.id}:zk"],
        "ExposedPorts" => {
          "9092/tcp" => {}
        },
        "Volumes" => {
          "/var/run/docker.sock" => {}
        },
        "Env" => [
          "KAFKA_ADVERTISED_HOST_NAME=#{DOCKER_HOSTNAME}",
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

    @kafka_brokers.each do |kafka|
      kafka.start(
        "PortBindings" => {
          "9092/tcp" => [{ "HostPort" => "" }]
        },
        "Binds" => ["/var/run/docker.sock:/var/run/docker.sock"],
      )
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

  def pause_kafka_broker(number)
    broker = @kafka_brokers[number]
    puts "Pausing broker #{number}"
    broker.pause
  end

  def unpause_kafka_broker(number)
    broker = @kafka_brokers[number]
    puts "Unpausing broker #{number}"
    broker.unpause
  end

  def start_kafka_broker(number)
    broker = @kafka_brokers[number]
    puts "Starting broker #{number}"
    broker.start
  end

  def create_topic(topic, num_partitions: 1, num_replicas: 1)
    command = [
      "/opt/kafka_2.10-0.8.2.0/bin/kafka-topics.sh",
      "--create",
      "--topic test-messages",
      "--replication-factor #{num_replicas}",
      "--partitions #{num_partitions}",
      "--zookeeper zk",
    ]

    puts "Creating topic #{topic}..."
    out, err, status = @kafka.exec(command)

    raise "Command failed: #{out}" if status != 0

    sleep 2
  end

  def stop
    puts "Stopping cluster..."

    @kafka_brokers.each {|kafka| kafka.stop rescue nil }
    @zookeeper.stop rescue nil
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

KAFKA_TOPIC = "test-messages"

KAFKA_CLUSTER = TestCluster.new
KAFKA_CLUSTER.start
KAFKA_CLUSTER.create_topic(KAFKA_TOPIC, num_partitions: 3, num_replicas: 2)

KAFKA_BROKERS = KAFKA_CLUSTER.kafka_hosts

host, port = KAFKA_BROKERS.first.split(":", 2)

KAFKA_HOST = host
KAFKA_PORT = port.to_i

at_exit {
  KAFKA_CLUSTER.stop
}
