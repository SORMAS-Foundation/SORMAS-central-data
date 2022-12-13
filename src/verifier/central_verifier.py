#
# usage: central_verifier.py [-h] [-H HOST] [-d DBNAME] [-u USERNAME]
#                           [-p PASSWORD] [-P PORT] [-i INPUT]
#
# options:
#  -h, --help            show this help message and exit
#  -H HOST, --host HOST  database server host or socket directory
#  -d DBNAME, --dbname DBNAME
#                        database name to connect to)
#  -u USERNAME, --username USERNAME
#                        database user name
#  -p PASSWORD, --password PASSWORD
#                        password for user
#  -P PORT, --port PORT  db portr
#  -i INPUT, --input INPUT
#                        path where to expect the central data community

import argparse
import json
import logging
import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

FORMAT = '%(message)s'

logFormatter = logging.Formatter(FORMAT)
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("output.log")
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--host", default=os.environ.get("host"), help="database server host or socket directory",
                    action="store")
parser.add_argument("-d", "--dbname", default=os.environ.get("dbname"), help="database name to connect to)",
                    action="store")
parser.add_argument("-u", "--username", default=os.environ.get("username"),
                    help="database user name",
                    action="store")
parser.add_argument("-p", "--password", default=os.environ.get("password"), help="password for user", action="store")
parser.add_argument("-P", "--port", default=os.environ.get("port"), help="db portr", action="store")
parser.add_argument("-i", "--input", default=os.environ.get("input"),
                    help="path where to expect the central data community",
                    action="store")

args, unknown = parser.parse_known_args()

assert len(unknown) == 0

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password} port={args.port}"
logging.info(f'Connecting to {args.host}')
PATH = args.input


def verify():
    table = 'community'
    with open(PATH) as f:
        entities: list = json.load(f)

    l = list(sorted(entities, key=lambda kv: kv['key']))
    length = len(l)
    for index, entity in enumerate(l):
        central_value = entity['value']
        name = central_value['name']
        id_ = central_value['externalID']
        uuid_ = central_value['uuid']
        logging.info(f"{index + 1}/{length}: Processing {name}, {id_}, {uuid_}")

        verify_uuid(table, central_value)


def insert_entity(table, central_value, conn):
    with conn.cursor(row_factory=dict_row) as cur:
        name: str = central_value['name']
        external_id: str = central_value['externalID']
        # force download again
        date = datetime.fromisoformat('2000-01-01').strftime("%Y-%m-%d %H:%M:%S")

        # fetch the max id from the community table
        max_id = cur.execute(f"SELECT max(id) FROM {table}").fetchone()['max']

        # fetch district id from district table by uuid
        district_id = \
            cur.execute(f"SELECT id FROM district WHERE uuid=%s", [central_value['district']['uuid']]).fetchone()[
                'id']

        cur.execute(f"""
        insert into public.community 
        (id, changedate, creationdate, name, uuid, district_id, archived, externalid,  centrally_managed, sys_period) 
        values ('{max_id + 1}','{date}', '{date}', %s, '{central_value['uuid']}', {district_id}, false, {external_id},true, '["{date}",)');
        """, (name,))
        logging.info(f"Inserted central value: {central_value}")


def verify_uuid(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            central_value_uuid = central_value['uuid']
            local = cur.execute(f"SELECT * FROM {table} WHERE uuid=%s", [central_value_uuid]).fetchone()
            if local:
                logging.info(f"\t\tExact local uuid match!")
            else:
                logging.info(f"\t\tNo exact local uuid match! Inserting {central_value}")
                insert_entity(table, central_value, conn)

            date = datetime.fromisoformat('2000-01-01').strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"UPDATE {table} SET changedate='{date}' WHERE uuid='{central_value_uuid}';")
            logging.info(f"\t\tUpdated changedate for {central_value_uuid}")


def has_default_name(table):
    return table in ['continent', 'subcontinent', 'country']


def main():
    verify()


if __name__ == '__main__':
    main()
