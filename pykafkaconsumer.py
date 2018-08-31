import argparse
import sys
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pprint import pprint

def read_messages():
    if ARGS.server:
        ARGS.server += ':9092'
    else:
        ARGS.server = 'kafka.int.janelia.org:9092,kafka2.int.janelia.org:9092,kafka3.int.janelia.org:9092'
    client = KafkaClient(ARGS.server, broker_version="1.0.0")
    ctopic = client.topics[ARGS.topic]
    if ARGS.offset == 'latest':
    	ARGS.offset = OffsetType.LATEST
    else:
    	ARGS.offset = OffsetType.EARLIEST
    consumer = ctopic.get_simple_consumer(consumer_group=ARGS.group,
    	                                  auto_offset_reset=ARGS.offset)
    for message in consumer:
        if message is not None:
            print ("[%s] %s:%d:%d: key=%s value=%s" % ('?',
                                                       ARGS.topic, message.partition_id,
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
    read_messages()
    sys.exit(0)
