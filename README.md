# IP Collector

## Overview

This project is an environment for developing an ip collector.  The setup consists of a producer of FlowRecords.  These records are transmitted as JSON over a Kafka Topic.

On the receiving end there is a Kafka Consumer that pulls the source and destintation IP out of the message.  A more complete example would model the message as a proper netflow record but this was enough to work through what I needed.

The consumer uses redis as a means of storing and deduping the list of IP addresses.  Redis is configured to backup to disk periodically and AOF is used to reduce data loss and improve performance during normal operation and if the process needs to be restarted.

## Requirements

* Python3
* Various pip modules in the requirements.txt
  confluent-kafka
  redis
  six
  requests
  fastavro
  jsonschema

* Docker && Docker Compose

## Setup

Setup your python environment and dependencies.

```
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
```

Run ```docker-compose up```.  This will bring up Redis and a simple single node Kafka cluster with the associated services.

Run ```python3 topic_setup.py``` to create the flows topic.

Connect to http://localhost:9021 and configure your topic retention.

## Producing FlowRecords

Run ```python3 flow_producer.py``` to start generating FlowRecords and publishing them to the flows topic.

## Consuming FlowRecords

Run ```python3 flow_consumer.py``` to start consuming the FlowRecords and adding the IPs to ip_address table in Redis.

Hit CTRL+c to stop consuming records.  You will be presented with the total number of records in the Redis table.

## Viewing entries in Redis

Run redis-cli.  From there you can run ```SCARD ipaddress``` to get the number of unique IP addresses found.  ```SMEMBERS ipaddress``` will give you a listing of the ip addresses.

## Shutting down

Type CTRL+c in each of the producer and consumer terminals to stop them.

Type CTRL+c in the docker-compose terminal to stop the services (I'm assuming you didn't add -d arg to docker-compose.  If you did run docker-compose stop it.)