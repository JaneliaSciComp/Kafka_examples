import argparse
from kafka import KafkaConsumer
import sys


def readMessages(server, topic):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=[server + ':9092'],
                             auto_offset_reset='smallest')
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Kafka consumer')
    parser.add_argument('-server', dest='server', default='kafka',
                        help='Server')
    parser.add_argument('-topic', dest='topic', default='test', help='Topic')
    args = parser.parse_args()
    readMessages(args.server, args.topic)
    sys.exit(0)
