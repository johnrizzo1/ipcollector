#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# This is a SerializingConsumer for flows published on kafka
# attempts to store a unique list in redis
#
import argparse

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

import redis


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


def dict_to_record(obj, ctx):
    """
    Converts object literal(dict) to a FlowRecord instance.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

        obj (dict): Object literal(dict)

    """
    if obj is None:
        return None

    return FlowRecord(src_ip=obj['src_ip'], dst_ip=obj['dst_ip'])


def main(args):
    topic = args.topic
    redis_table = args.redis_table
    redis_server = args.redis_server

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
    json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_record)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'key.deserializer': string_deserializer,
        'value.deserializer': json_deserializer,
        'group.id': args.group,
        'auto.offset.reset': "earliest"
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])
    r = redis.Redis(host=redis_server, port=6379, db=0)

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record = msg.value()
            if record is not None:
                print("FlowRecord {}: src_ip: {}\tdst_ip: {}\n"
                      .format(msg.key(), record.src_ip, record.dst_ip))
                r.sadd(redis_table, record.src_ip)
                r.sadd(redis_table, record.dst_ip)
        except KeyboardInterrupt:
            print("\nUnique IPs: {}".format(r.scard(redis_table)))
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IP DeserializingConsumer")
    parser.add_argument('-b', dest="bootstrap_servers", default="localhost",
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="flows", help="Topic name")
    parser.add_argument('-g', dest="group", default="flow-consumer", help="Consumer group")
    parser.add_argument('-r', dest="redis_server", default="localhost", help="Redis Server")
    parser.add_argument('-i', dest="redis_table", default="ipaddress", 
                        help="Redis table to store/read IP address'")

    main(parser.parse_args())
