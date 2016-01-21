#!/bin/bash

docker-compose run kafka1 /opt/kafka_2.10-0.8.2.0/bin/kafka-topics.sh --create --topic test-messages --replication-factor 3 --partitions 5 --zookeeper zk
