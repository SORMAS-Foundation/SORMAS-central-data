import argparse
import json
import logging
import os
import random
import uuid
import itertools

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
parser.add_argument("-i", "--input", default=os.environ.get("input"), help="path where to expect the central data",
                    action="store")
parser.add_argument("-o", "--output", default=os.environ.get("OUTPUT"), help="path of the log file", action="store")
parser.add_argument("-b", "--begin", default=os.environ.get("BEGIN"), help="uuid from where to start", action="store")

args, unknown = parser.parse_known_args()

assert len(unknown) == 0

fileHandler = logging.FileHandler(args.output)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password} port={args.port}"


def invalidate_centrally_managed_data():
    rnd = random.Random()
    rnd.seed(43)

    with open(args.input) as f:
        entities: list = json.load(f)
        sorted_list = list(sorted(entities, key=lambda kv: kv['key']))
        length = len(sorted_list)
        assert length == 13372

    begin_item = list(filter(lambda x: x['key'] == args.begin, sorted_list))
    if len(begin_item) != 1:
        raise Exception(f"Begin item {args.begin} not found")

    tail_list = list(itertools.dropwhile(lambda x: x['key'] != args.begin, sorted_list))

    diff_length = length - len(tail_list)

    head_list = sorted_list[:diff_length]

    assert len(head_list) + len(tail_list) == 13372
    assert tail_list[0]['key'] == begin_item[0]['key']

    logging.info(f"Dropped {diff_length} items")
    logging.info(f'Connecting to {CONNECTION}')
    with psycopg.connect(CONNECTION) as conn:
        cur = conn.cursor(row_factory=dict_row)

        for index, entity in enumerate(tail_list):
            logging.info(f"{index + diff_length + 1}/{length}: Processing {entity['value']}")
            entity = entity['value']
            # generate a new uuid for the record
            new_uuid = str(uuid.UUID(int=rnd.getrandbits(128), version=4))
            # update the record with the new uuid, archive it, and invalidate the name and the external id
            cur.execute(
                f"UPDATE community "
                f"SET uuid = '{new_uuid}', archived = TRUE, centrally_managed=FALSE,"
                f"name = %s, externalid = %s "
                f"WHERE uuid = %s "
                f"RETURNING id, name, uuid, externalid, archived, centrally_managed",
                (entity['name'] + '_INVALID', entity['externalID'] + '_INVALID_' + str(index), entity['uuid']))

            update = cur.fetchone()
            logging.info(f'updated row to {update}')


def main():
    invalidate_centrally_managed_data()


if __name__ == '__main__':
    main()
