import argparse
import random
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
        ARGS.group = 'temporary_' + str(time.time()) + '_' + str(random.randint(1,1001))
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
        timestamp = int(float(ARGS.timestamp) * 1000)
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
    counter = 1
    end_stamp = int(float(ARGS.end_timestamp) * 1000) if ARGS.end_timestamp else 0
    for message in consumer:
        if ARGS.debug:
            pprint(message)
        if ARGS.end_timestamp and int(message.timestamp) > end_stamp:
            sys.exit(0)
        try:
            print ("[%s] %s:%d:%d: key=%s value=%s" % (datetime.fromtimestamp(message.timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                       message.topic, message.partition,
                                                       message.offset, message.key,
                                                       message.value))
            if ARGS.limit and (counter >= int(ARGS.limit)):
                sys.exit(0)
            counter += 1
        except UnicodeDecodeError:
            print("[%s] %s:%d:%d: key=%s CANNOT DECODE MESSAGE" % (datetime.fromtimestamp(message.timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f'),
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
    PARSER.add_argument('--limit', dest='limit', default=0, help='Number of messages to consume')
    PARSER.add_argument('--timestamp', dest='timestamp', default='', help='Timestamp (Epoch sec)')
    PARSER.add_argument('--end_timestamp', dest='end_timestamp', default='', help='End timestamp (Epoch sec)')
    PARSER.add_argument('--offset', dest='offset', default='earliest',
                        help='offset (earliest, latest, or offset#)')
    PARSER.add_argument('--debug', dest='debug', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARGS = PARSER.parse_args()
    read_messages()
    sys.exit(0)
