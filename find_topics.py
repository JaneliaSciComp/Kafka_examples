import argparse
import sys
import colorlog
import requests
import kafka

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
SERVER = ''

def call_responder(server, endpoint):
    """ Call a responder and return JSON
        Keyword arguments:
          server: server
          endpoint: endpoint
        Returns:
          JSON
    """
    url = CONFIG[server]['url'] + endpoint
    LOGGER.debug(url)
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code == 200:
        try:
            return req.json()
        except requests.exceptions.RequestException as err:
            LOGGER.error(err)
            sys.exit(-1)
        except Exception as err: #pylint: disable=W0703
            temp = "An exception of type %s occurred. Arguments:\n%s"
            LOGGER.error(temp, type(err).__name__, err.args)
            sys.exit(-1)
    LOGGER.error('Status: %s', str(req.status_code))
    sys.exit(-1)


def initialize_program():
    """ Initialize
    """
    global CONFIG, SERVER# pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/servers')
    SERVER = data['config']


def show_topics():
    key = 'Kafka' if ARG.MANIFOLD == 'prod' else 'Kafka-dev'
    consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=[SERVER[key]['address']])
    topics = consumer.topics()
    broker_list = ','.join(SERVER[key]['broker_list'])
    for top in sorted(topics):
        print(top)
        print("sudo bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "
              + broker_list + " --topic %s" % (top))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Kafka consumer')
    PARSER.add_argument('--manifold', dest='MANIFOLD', default='prod',
                        help='Manifold (prod or dev)')
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
    initialize_program()
    show_topics()
    sys.exit(0)
