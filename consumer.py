import argparse
import sys
import time
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
from pprint import pprint


def read_messages():
    if ARGS.server:
        server_list = [ARGS.server + ':9092']
    else:
        server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
    if not ARGS.group:
        ARGS.group = None
    try:
        offsetnum = int(ARGS.offset)
        autooffset = 'earliest'
        ARGS.group = 'temporary' + str(time.time())
    except ValueError:
        offsetnum = None
        autooffset = ARGS.offset
    consumer = KafkaConsumer(ARGS.topic,
                             bootstrap_servers=server_list,
                             group_id=ARGS.group,
                             auto_offset_reset=autooffset,
                             consumer_timeout_ms=int(5000))
    toppart = TopicPartition(ARGS.topic, 0)
    if ARGS.timestamp:
        timestamp = int(ARGS.timestamp) * 1000
        timestamps = {toppart: timestamp}
        offsethash = consumer.offsets_for_times(timestamps)
        offsetnum = offsethash[toppart].offset
        if ARGS.debug:
            print("Offset for " + str(timestamp) + " is " + str(offsetnum))
        consumer.seek(toppart, int(offsetnum))
    elif offsetnum:
        for msg in consumer:
            consumer.seek(toppart, int(offsetnum))
            consumer.commit()
            break
    for message in consumer:
        if ARGS.debug:
            pprint(message)
        try:
            print ("[%s] %s:%d:%d: key=%s value=%s" % (datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S'),
                                                       message.topic, message.partition,
                                                       message.offset, message.key,
                                                       message.value))
            sys.exit(0)
        except UnicodeDecodeError:
            print("[%s] %s:%d:%d: key=%s CANNOT DECODE MESSAGE" % (datetime.fromtimestamp(message.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S'),
                                                                   message.topic, message.partition,
                                                                   message.offset, message.key))
            pprint(message)
            sys.exit(-1)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            sys.exit(-1)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
    PARSER.add_argument('--group', dest='group', default='', help='Group')
    PARSER.add_argument('--timestamp', dest='timestamp', default='', help='Timestamp (Epoch sec)')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest, latest, or offset#)')
    PARSER.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARGS = PARSER.parse_args()
    read_messages()
    sys.exit(0)
