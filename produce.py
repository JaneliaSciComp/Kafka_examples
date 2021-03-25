import argparse
import json
from random import randint
import sys
from time import gmtime, strftime, sleep
import colorlog
from kafka import KafkaProducer
from kafka.errors import KafkaError


def produce_messages():
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             key_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             bootstrap_servers=ARG.SERVERS)
    messagenum = 1
    while True:
        future = producer.send('test', 'Periodic message ' + str(messagenum)
                               + ' ' + strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime()))
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as err:
            LOGGER.error(str(err))
        print('Message:   %d' % (messagenum))
        LOGGER.info('Topic:     %s', record_metadata.topic)
        LOGGER.info('Partition: %s', record_metadata.partition)
        LOGGER.info('Offset:    %s', record_metadata.offset)
        messagenum += 1
        sleep(randint(1, 4))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--servers', dest='SERVERS',
                        default='kafka-dev.int.janelia.org:9092,'
                        + 'kafka-dev.int.janelia.org:9093,kafka-dev.int.janelia.org:9094',
                        help='Bootstrap servers (comma-separated)')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)
    produce_messages()
    sys.exit(0)
