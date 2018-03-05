import argparse
import sys
from pykafka import KafkaClient
from pykafka.common import OffsetType


def read_messages(server, topic, group, offset):
    if server:
        server += ':9092'
    else:
        server = 'kafka:9092,kafka2:9092,kafka3:9092'
    client = KafkaClient(server)
    ctopic = client.topics[topic]
    if offset == 'latest':
    	offset = OffsetType.LATEST
    else:
    	offset = OffsetType.EARLIEST
    consumer = ctopic.get_simple_consumer(consumer_group=group,
    	                                  auto_offset_reset=offset)
    for message in consumer:
        if message is not None:
            print ("%s:%d:%d: key=%s value=%s" % (topic, message.partition_id,
                                                  message.offset,
                                                  message.partition_key,
                                                  message.value))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--group', dest='group', default=None, help='Group')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest or latest)')
    ARGS = PARSER.parse_args()
    read_messages(ARGS.server, ARGS.topic, ARGS.group, ARGS.offset)
    sys.exit(0)
