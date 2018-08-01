import argparse
import sys
from kafka import KafkaConsumer


def read_messages(server, topic, group, offset):
    if server:
        server_list = [server + ':9092']
    else:
        server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
    if not group:
        group = None
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=server_list,
                             group_id=group,
                             auto_offset_reset=offset)
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--group', dest='group', default='', help='Group')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest or latest)')
    ARGS = PARSER.parse_args()
    read_messages(ARGS.server, ARGS.topic, ARGS.group, ARGS.offset)
    sys.exit(0)
