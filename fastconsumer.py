import argparse
import sys
from confluent_kafka import Consumer, KafkaError


def read_messages(server, topic, group, offset):
    if not server:
        server = 'kafka,kafka2,kafka3'
    if not group:
        group = None
    cons = Consumer({'bootstrap.servers': server, 'group.id': group,
                     'default.topic.config': {'auto.offset.reset': offset}})
    cons.subscribe([topic])
    running = True
    while running:
        msg = cons.poll()
        if not msg.error():
            print ("%s:%s:%d: key=%s value=%s" % (msg.topic(), msg.partition(),
                                                  msg.offset(),
                                                  str(msg.key()),
                                                  msg.value().decode('utf-8')))
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
    cons.close()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer (confluent_kafka)')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--group', dest='group', default='', help='Group')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest or latest)')
    ARGS = PARSER.parse_args()
    read_messages(ARGS.server, ARGS.topic, ARGS.group, ARGS.offset)
    sys.exit(0)
