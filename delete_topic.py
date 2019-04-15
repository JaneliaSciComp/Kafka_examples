"""
delete_topic.py
Delete a Kafka topic.
BEWARE: deletion is forever! If you delete a topic,
you ain't getting it back!
"""
import argparse
import sys
import colorlog
from kafka.client_async import KafkaClient
from kafka.errors import KafkaError
from kafka.protocol import admin
import requests


# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}


def call_responder(server, endpoint):
    """
    Call a responder
    Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code not in [200, 404]:
        LOGGER.critical('Status: %s', str(req.status_code))
        sys.exit(-1)
    else:
        return req.json()


def delete_topic():
    """
    Delete the specified topic
    """
    if ARG.server:
        ARG.server += ':9092'
    else:
        ARG.server = ','.join(SERVER['Kafka']['broker_list'])
    client = KafkaClient(bootstrap_servers=ARG.server)
    try:
        topic_req = admin.DeleteTopicsRequest_v1(topics=[ARG.topic], timeout=1000)
        future = client.send(client.least_loaded_node(), topic_req)
        client.poll(timeout_ms=100, future=future)
        result = future.value
        LOGGER.debug(result)
        error_code = result.topic_error_codes[0][1]
        if error_code:
            LOGGER.critical('Could not delete topic %s, error code=%d', ARG.topic, error_code)
            sys.exit(error_code)
        else:
            print("Deleted topic %s" % (ARG.topic))
    except KafkaError:
        LOGGER.critical("Could not delete topic %s", ARG.topic)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer (confluent_kafka)')
    PARSER.add_argument('--server', dest='server', default='', help='Server')
    PARSER.add_argument('--topic', dest='topic', default='test', help='Topic')
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

    # Initialize
    CONFIG = call_responder('config', 'config/rest_services')['config']
    SERVER = call_responder('config', 'config/servers')['config']

    # Delete topic
    delete_topic()
    sys.exit(0)
