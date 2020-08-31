#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# This is a simple IP Address SerializingProducer using JSON.
#
import argparse
from uuid import uuid4

from six.moves import input

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from random import randint, choice, getrandbits
from string import hexdigits
from ipaddress import IPv4Network, IPv4Address

class FlowRecord(object):
    """
    A Netflow v8 record

    Args:
        src_ip (str): Source IP
        dst_ip (str): Destination IP

    """
    def __init__(self, src_ip, dst_ip):
        self.src_ip = src_ip
        self.dst_ip = dst_ip


def record_to_dict(record, ctx):
    """
    Returns a dict representation of a FlowRecord instance for serialization.

    Args:
        record (FlowRecord): FlowRecord Instance.
        ctx (SerializationContext): Metadata pertaining to the serialization operation
    
    Returns:
        dict: Dict populated with user attributes to be serialized

    """
    return dict(src_ip=record.src_ip,
                dst_ip=record.dst_ip)


def random_ip(subnets):
    # network containing all addresses from 10.0.0.0 to 10.0.0.255
    subnet = subnets[randint(0, len(subnets)-1)]

    # subnet.max_prefixlen contains 32 for IPv4 subnets and 128 for IPv6 subnets
    # subnet.prefixlen is 24 in this case, so we'll generate only 8 random bits
    bits = getrandbits(subnet.max_prefixlen - subnet.prefixlen)

    # here, we combine the subnet and the random bits
    # to get an IP address from the previously specified subnet
    addr = IPv4Address(subnet.network_address + bits)
    addr_str = str(addr)

    # print(addr_str)
    return addr_str


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for FlowRecord record {}: {}".format(msg.key(), err))
        return
    print('FlowRecord {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "FlowRecord",
      "description": "A netflow record coming from Kafka",
      "type": "object",
      "properties": {
        "src_ip": {
          "description": "Source IP",
          "type": "string"
        },
        "dst_ip": {
          "description": "Destination IP",
          "type": "string"
        }
      },
      "required": [ "src_ip", "dst_ip" ]
    }
    """

    subnets = [
        IPv4Network("10.10.10.0/24"),
        IPv4Network("10.20.20.0/24"),
        IPv4Network("10.30.30.0/24"),
        IPv4Network("10.40.40.0/24"),
        IPv4Network("10.124.0.0/16"),
        IPv4Network("192.168.0.0/16")]

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, record_to_dict)

    producer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing flow records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            record = FlowRecord(src_ip=random_ip(subnets),
                                dst_ip=random_ip(subnets))
            producer.produce(topic=topic, key=str(uuid4()),
                            value=record, on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IP Address SerializingProducer")
    parser.add_argument('-b', dest="bootstrap_servers", default="localhost",
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", default="http://localhost:8081",
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="flows", help="Topic name")

    main(parser.parse_args())
