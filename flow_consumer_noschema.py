#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# This is a SerializingConsumer for flows published on kafka
# attempts to store a unique list in redis
#
import argparse
import json
import logging
import sys
import redis

from pprint import pprint
from confluent_kafka import Consumer

msg_count = 0

class FlowRecord(object):
    """
    A Netflow v8 record

    Args:
        ip_src (str): Source IP
        ip_dst (str): Destination IP

    """
    def __init__(self, ip_src, ip_dst):
        self.ip_src = ip_src
        self.ip_dst = ip_dst


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

    return FlowRecord(ip_src=obj['ip_src'], ip_dst=obj['ip_dst'])


def main(args):
    topic = args.topic
    redis_table = args.redis_table
    redis_server = args.redis_server
    # msg_count = 0

    consumer_conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        # 'auto.offset.reset': "earliest"
        'auto.offset.reset': "latest"
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    r = redis.Redis(host=redis_server, port=6379, db=0)

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record = json.loads(msg.value())
            if record is not None:
                logging.info("FlowRecord {}: ip_src: {}\tip_dst: {}"
                    .format(msg.key(), record['ip_src'], record['ip_dst']))
                r.sadd(redis_table, record['ip_src'])
                r.sadd(redis_table, record['ip_dst'])
        except KeyboardInterrupt:
            logging.info("\nUnique IPs: {}".format(r.scard(redis_table)))
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IP DeserializingConsumer")
    parser.add_argument('-b', dest="bootstrap_servers", default="localhost",
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-t', dest="topic", default="pmacct.acct", help="Topic name")
    parser.add_argument('-g', dest="group", default="flow-consumer", help="Consumer group")
    parser.add_argument('-r', dest="redis_server", default="localhost", help="Redis Server")
    parser.add_argument('-i', dest="redis_table", default="ipaddress", 
                        help="Redis table to store/read IP address'")

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    main(parser.parse_args())
