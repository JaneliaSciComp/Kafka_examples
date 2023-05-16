import argparse
from datetime import datetime, timedelta
import os
import sys
import colorlog
from kafka import KafkaConsumer, TopicPartition
from time import ctime, localtime, strftime
from tqdm import tqdm


COUNT = {"topics": 0, "timestamp": 0, "empty": 0, "old": 0, "current": 0}

def read_messages():
    if ARG.SERVER:
        server_list = [ARG.SERVER + ':9092']
    else:
        server_list = ['kafka.int.janelia.org:9092', 'kafka2.int.janelia.org:9092', 'kafka3.int.janelia.org:9092']
    if not ARG.GROUP:
        ARG.GROUP = None
    consumer = KafkaConsumer(bootstrap_servers=server_list,
                             auto_offset_reset=ARG.OFFSET,
                             consumer_timeout_ms=int(5000))
    topics = consumer.topics()
    for topic in tqdm(sorted(topics)):
        COUNT['topics'] += 1
        if ARG.QUICK:
            print(topic)
            continue
        parts = consumer.partitions_for_topic(topic)
        if parts:
            partitions = [TopicPartition(topic, p) for p in parts]
            eoff = consumer.end_offsets(partitions)
            maxoff = 0
            partnum = -1
            for key in eoff:
                if eoff[key] > maxoff:
                    maxoff = eoff[key]
                    partnum = key.partition
            if not maxoff:
                EMPTY.write("%s\n" % (topic))
                EMPTY.flush()
                COUNT['empty'] += 1
                continue
            part = TopicPartition(topic, 0)
            consumer.assign([part])
            consumer.seek(part, maxoff-1)
            for msg in consumer:
                if msg.timestamp == -1:
                    ERROR.write("%s: %s\n" % (topic, msg))
                    COUNT['timestamp'] += 1
                    break
                today = datetime.today()
                delta = (today - datetime.fromtimestamp(msg.timestamp/1000)).days
                timestr = strftime("%Y-%m-%d %H:%M:%S %Z", localtime(msg.timestamp/1000))
                if delta >= 365:
                    OUTPUT.write("%s\t%s\t%s\n" % (topic, timestr, delta))
                    OUTPUT.flush()
                    COUNT['old'] += 1
                else:
                    COUNT['current'] += 1
                break
    print("Topics:                   %d" % (COUNT['topics']))
    print("Topics >= 1 year old:     %d" % (COUNT['old']))
    print("Topics < 1 year old:      %d" % (COUNT['current']))
    print("Empty topics:             %d" % (COUNT['empty']))
    print("Topics missing timestamp: %d" % (COUNT['timestamp']))
    EMPTY.close()
    if not COUNT['empty']:
        os.remove(EMPTY_FILE)
    ERROR.close()
    if not COUNT['timestamp']:
        os.remove(ERROR_FILE)
    OUTPUT.close()
    if not COUNT['old']:
        os.remove(OUTPUT_FILE)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka topic counter')
    PARSER.add_argument('--server', dest='SERVER', default='', help='Server')
    PARSER.add_argument('--topic', dest='TOPIC', default='test', help='Topic')
    PARSER.add_argument('--group', dest='GROUP', default='', help='Group')
    PARSER.add_argument('--offset', dest='OFFSET', default='earliest',
                        help='offset (earliest or latest)')
    PARSER.add_argument('--quick', dest='QUICK', action='store_true',
                        default=False, help='Quick mode')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    EMPTY_FILE = 'topic_empty.txt'
    EMPTY = open(EMPTY_FILE, 'w')
    ERROR_FILE = 'topic_error.txt'
    ERROR = open(ERROR_FILE, 'w')
    OUTPUT_FILE = 'topic_aging.txt'
    OUTPUT = open(OUTPUT_FILE, 'w')
    read_messages()
    sys.exit(0)
