import argparse
import json
import os
import re
import sys
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
from pprint import pprint


def read_messages():
    if ARGS.server:
        ARGS.server += ':9092'
    else:
        ARGS.server = 'kafka.int.janelia.org,kafka2.int.janelia.org,kafka3.int.janelia.org'
    if not ARGS.group:
        ARGS.group = 'dvid_activity'
        ARGS.group = None
    parms = {'bootstrap.servers': ARGS.server, 'default.topic.config': {'auto.offset.reset': ARGS.offset}}
    if ARGS.group:
        parms.update({'group.id': ARGS.group})
    cons = Consumer(parms)
    cons.subscribe([ARGS.topic])
    running = True
    while running:
        msg = cons.poll()
        if not msg.error():
            ts = int(msg.timestamp()[1])
            if ARGS.debug:
                print ("[%s] %s:%s:%d: key=%s value=%s" % (datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'),
                                                           msg.topic(), msg.partition(),
                                                           msg.offset(),
                                                           str(msg.key()),
                                                           msg.value().decode('utf-8')))
            message = json.loads(msg.value().decode('utf-8'))
            if 'duration' in message:
                print(json.dumps(message))
            if 'Durationx' in message:
                match = re.search(r"([0-9.]+)(.+)", message['Duration'])
                if match.group(2) == 's':
                    duration = float(match.group(1)) * 1000
                elif match.group(2) == 'ms':
                    duration = float(match.group(1))
                else:
                    duration = float(match.group(1)) / 1000
                print(match.group(), ' ', duration)
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
    cons.close()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer (confluent_kafka)')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--group', dest='group', default=os.getpid(), help='Group')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest or latest)')
    PARSER.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARGS = PARSER.parse_args()
    read_messages()
    sys.exit(0)
