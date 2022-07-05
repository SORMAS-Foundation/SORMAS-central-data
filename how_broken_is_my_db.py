# usage: how_broken_is_my_db.py [-h] [-H HOST] [-d DBNAME] [-u USERNAME]
#                               [-p PASSWORD] [-P PORT] [-o OUTPUT]
#
# options:
#   -h, --help            show this help message and exit
#   -H HOST, --host HOST  database server host or socket directory
#   -d DBNAME, --dbname DBNAME
#                         database name to connect to)
#   -u USERNAME, --username USERNAME
#                         database user name
#   -p PASSWORD, --password PASSWORD
#                         password for user
#   -P PORT, --port PORT  port to connect to
#   -o OUTPUT, --output OUTPUT
#                         path of the log file

import argparse
import logging
import os

import psycopg
from psycopg.rows import dict_row

FORMAT = '%(message)s'

logFormatter = logging.Formatter(FORMAT)
rootLogger = logging.getLogger()

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--host", default=os.environ.get("HOST"), help="database server host or socket directory",
                    action="store")
parser.add_argument("-d", "--dbname", default=os.environ.get("DBNAME"), help="database name to connect to)",
                    action="store")
parser.add_argument("-u", "--username", default=os.environ.get("USERNAME"),
                    help="database user name",
                    action="store")
parser.add_argument("-p", "--password", default=os.environ.get("PASSWORD"), help="password for user", action="store")

parser.add_argument("-P", "--port", default=os.environ.get("PORT"), help="port to connect to", action="store")
parser.add_argument("-o", "--output", default=os.environ.get("OUTPUT"), help="path of the log file", action="store")

args, unknown = parser.parse_known_args()

assert len(unknown) == 0

fileHandler = logging.FileHandler("/tmp/output.log")
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password} port={args.port}"


def has_default_name(table):
    return table in ['continent', 'subcontinent', 'country']


def report_duplicates():
    logging.info(f'Connecting to {CONNECTION}')
    with psycopg.connect(CONNECTION) as conn:
        for table in ['continent', 'subcontinent', 'country', 'region', 'district', 'community']:
            cur = conn.cursor(row_factory=dict_row)
            name = 'defaultName' if has_default_name(table) else 'name'
            # list duplicates in column externalid
            cur.execute(
                f"SELECT id, {name}, uuid, archived, centrally_managed, externalid  FROM {table} WHERE externalid IN (SELECT externalid FROM {table} GROUP BY externalid HAVING COUNT(*) > 1) ORDER BY externalid DESC ")
            rows = cur.fetchall()
            if len(rows) > 0:
                logging.info(f'{table} has {len(rows)} duplicates')
                for row in rows:
                    logging.info(f'\t{table} {row}')
            else:
                logging.info(f'{table} has no duplicates')


def main():
    report_duplicates()


if __name__ == '__main__':
    main()
