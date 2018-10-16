import argparse
import json
import sys
import colorlog
import requests
import MySQLdb
from business_duration import businessDuration
import pandas as pd
import datetime
import holidays as pyholidays
import time

# Database
READ = {'all': "SELECT id,user,annotation,type,UNIX_TIMESTAMP(start_date),"
               + "UNIX_TIMESTAMP(complete_date),"
               +  "UNIX_TIMESTAMP(complete_date)-UNIX_TIMESTAMP(start_date) "
               + "FROM assignment_vw WHERE start_date > 0 ORDER BY start_date",
        'completed': "SELECT id,user,annotation,type,UNIX_TIMESTAMP(start_date),"
                     + "UNIX_TIMESTAMP(complete_date),"
                     + "UNIX_TIMESTAMP(complete_date)-UNIX_TIMESTAMP(start_date) "
                     + "FROM assignment_vw WHERE is_complete=1 AND UNIX_TIMESTAMP(complete_date)>0"
       }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        logger.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        logger.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(db):
    """ Connect to a database
        Keyword arguments:
        db: database dictionary
    """
    logger.debug("Connecting to %s on %s", db['name'], db['host'])
    try:
        conn = MySQLdb.connect(host=db['host'], user=db['user'],
                               passwd=db['password'], db=db['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as err:
        sql_error(err)


def call_responder(server, endpoint, allow_404 = False):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if (req.status_code == 200) or (req.status_code == 404 and allow_404):
        return req.json()
    else:
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)


def initialize_program():
    """ Initialize databases """
    global CONFIG
    dbc = call_responder('config', 'config/db_config')
    data = dbc['config']
    (CONN['mad'], CURSOR['mad']) = db_connect(data['mad']['prod'])
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def process_mad(filename):
    open_time = datetime.time(6, 0, 0)
    close_time = datetime.time(18, 0, 0)
    holidaylist = pyholidays.US()
    try:
        CURSOR['mad'].execute(READ['all'], )
    except MySQLdb.Error as err:
        sql_error(err)
    handle = open(filename, 'w') if filename else sys.stdout
    rows = CURSOR['mad'].fetchall()
    workdict = dict()
    for row in rows:
        if row[5] and (row[5] < row[4] or not row[4]):
            continue
        if (not row[5]) and ARG.COMPLETED:
            continue
        if (row[5]) and not ARG.COMPLETED:
            continue
        org = ''
        if row[1] in workdict:
            org = workdict[row[1]]
        else:
            workday = call_responder('config', 'config/workday/' + row[1], True)
            if 'config' in workday and 'organization' in workday['config']:
                org = workday['config']['organization']
            workdict[row[1]] = org
        completed = 1 if row[5] else 0
        duration = row[6] if (row[6] > 0) else 0
        if row[5]:
            startstring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[4]))
            endstring = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[5]))
            startdate = pd.to_datetime(startstring)
            enddate = pd.to_datetime(endstring)
            working_duration = businessDuration(startdate,enddate,open_time,close_time,
                                                holidaylist=holidaylist,unit='hour') * 3600.0
            logger.debug("Working duration %s-%s is %f hr" % (startdate, enddate, working_duration))
        else:
            working_duration = 0
        rec = {'id': row[0], 'user': row[1], 'organization': org,
               'annotation': row[2],'type': row[3],
               'start_time': row[4], 'end_time': row[5], 'duration': duration,
               'working_duration': working_duration
               }
        handle.write(json.dumps(rec) + "\n")
    if handle is not sys.stdout:
        handle.close()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Dump asssignments from MAD to a JSON file")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='File containing assignments')
    PARSER.add_argument('--completed', dest='COMPLETED', action='store_true',
                        default=False, help='Completed assignments only')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    process_mad(ARG.FILE)
    sys.exit(0)
