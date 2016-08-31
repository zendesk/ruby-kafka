require "docker"

if ENV.key?("DOCKER_HOST")
  Docker.url = ENV.fetch("DOCKER_HOST")
end

class TestCluster
  DOCKER_HOST = ENV.fetch("DOCKER_HOST") {
    ip = if `ifconfig -a | grep '^[a-zA-Z]' | cut -d':' -f1`.split.include?('docker0')
           # Use the IP address of the docker0 network interface
           `/sbin/ifconfig docker0 | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1`.strip
         else
           # Use the IP address of the current machine. The loopback address won't resolve
           # properly within the container.
           `/sbin/ifconfig | grep -v '127.0.0.1' | awk '$1=="inet" {print $2}' | cut -f1 -d'/' | head -n 1`.strip
         end
    "kafka://#{ip}"
  }

  DOCKER_HOSTNAME = URI(DOCKER_HOST).host
  KAFKA_IMAGE = "ches/kafka:0.10.0.0"
  ZOOKEEPER_IMAGE = "jplock/zookeeper:3.4.6"
  KAFKA_CLUSTER_SIZE = 3

  def start
    [KAFKA_IMAGE, ZOOKEEPER_IMAGE].each do |image|
      print "Fetching image #{image}... "

      unless Docker::Image.exist?(image)
        Docker::Image.create("fromImage" => image)
      end

      puts "OK"
    end

    puts "Starting cluster..."

    @zookeeper = create(
      "Image" => ZOOKEEPER_IMAGE,
      "Hostname" => "localhost",
      "ExposedPorts" => {
        "2181/tcp" => {}
      },
    )

    @kafka_brokers = KAFKA_CLUSTER_SIZE.times.map {|broker_id|
      port = 9093 + broker_id

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

    start_zookeeper_container
    start_kafka_containers
  end

  def start_zookeeper_container
    @zookeeper.start(
      "PortBindings" => {
        "2181/tcp" => [{ "HostPort" => "" }]
      }
    )

    config = @zookeeper.json.fetch("NetworkSettings").fetch("Ports")
    port = config.fetch("2181/tcp").first.fetch("HostPort")

    wait_for_port(port)

    Thread.new do
      File.open("zookeeper.log", "a") do |log|
        @zookeeper.attach do |stream, chunk|
          log.puts(chunk)
        end
      end
    end
  end

  def start_kafka_containers
    @kafka_brokers.each_with_index do |kafka, index|
      port = 9093 + index

      kafka.start(
        "PortBindings" => {
          "9092/tcp" => [{ "HostPort" => "#{port}/tcp" }]
        },
      )

      Thread.new do
        File.open("kafka#{index}.log", "a") do |log|
          kafka.attach do |stream, chunk|
            log.puts(chunk)
          end
        end
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
    print "Creating topic #{topic}... "

    kafka_command [
      "/kafka/bin/kafka-topics.sh",
      "--create",
      "--topic=#{topic}",
      "--replication-factor=#{num_replicas}",
      "--partitions=#{num_partitions}",
      "--zookeeper=zookeeper",
    ]

    puts "OK"

    out = kafka_command [
      "/kafka/bin/kafka-topics.sh",
      "--describe",
      "--topic=#{topic}",
      "--zookeeper=zookeeper",
    ]
    puts out
  end

  def kafka_command(command)
    container = create(
      "Image" => KAFKA_IMAGE,
      "Links" => ["#{@zookeeper.id}:zookeeper"],
      "Cmd" => command,
    )

    begin
      container.start
      status = container.wait.fetch('StatusCode')

      if status != 0
        puts container.logs(stdout: true, stderr: true)
        raise "Command failed with status #{status}"
      end

      container.logs(stdout: true, stderr: true)
    ensure
      container.delete(force: true) rescue nil
    end
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

      wait_for_port(port, host: host)
    end
  end

  def wait_for_port(port, host: DOCKER_HOSTNAME)
    print "Waiting for #{host}:#{port}..."

    loop do
      begin
        socket = TCPSocket.open(host, port)
        socket.close
        puts " OK"
        break
      rescue
        print "."
        sleep 1
      end
    end
  end

  def create(options)
    Docker::Container.create(options)
  end
end
